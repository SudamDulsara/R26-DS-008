"""
The two readers — Tesseract for the raw side, a fine-tuned model for the
corrected side.

Pipeline B reads every page TWICE. That is the whole design:

    page image ──> Tesseract        ──> raw text        (~11% CER)
              └──> fine-tuned OCR   ──> corrected text  (target ~1% CER)

There is no text-to-text correction step. The corrected side is produced by
reading the picture again with a better reader, not by repairing Tesseract's
output. That is what makes the pairs realistic: the errors on the raw side
come from a real OCR engine on real scanned paper, rather than from
artificially degrading clean text.

Both readers return NORMALISED text. Every measurement in this project is made
on NFC-composed text, and two valid encodings of the same Sinhala word scored
CER 0.5 through raw jiwer once — 50% error on text that was already correct.
Normalising here means nothing downstream can forget to.
"""

import os
import sys
import time

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from pipeline.normalize import normalize          # noqa: E402


# ══════════════════════════════════════════════════════════════
#  The raw side
# ══════════════════════════════════════════════════════════════

class TesseractReader:
    """
    Tesseract with the Sinhala language pack.

    Measured at 0.1079 CER over the 202 human-transcribed acts-1010 test
    pages, which is where the "raw" side's error rate comes from. Error is
    strongly era-dependent: 0.1650 on 1981-89 scans against 0.0688 on
    2000-2019 print.
    """

    name = "tesseract"

    def __init__(self, lang: str = "sin"):
        import pytesseract
        self._pt = pytesseract
        self.lang = lang
        try:
            self.version = str(pytesseract.get_tesseract_version())
        except Exception as exc:                    # not installed / not on PATH
            raise RuntimeError(
                "Tesseract is not available. Install it system-wide with the "
                "'sin' language pack; see pipeline/ocr_engine.py for the "
                "Windows install paths this project expects."
            ) from exc

        langs = set(pytesseract.get_languages())
        if lang not in langs:
            raise RuntimeError(
                f"Tesseract has no '{lang}' language data. Installed: "
                f"{sorted(langs)[:10]}..."
            )

    def read(self, image) -> tuple:
        """Return (text, seconds)."""
        t0 = time.time()
        text = self._pt.image_to_string(image, lang=self.lang)
        return normalize(text), time.time() - t0


# ══════════════════════════════════════════════════════════════
#  The corrected side
# ══════════════════════════════════════════════════════════════

class PassthroughCorrector:
    """
    Returns the raw text unchanged.

    Exists so the pipeline can be built, tested and demonstrated before the
    fine-tuned model is ready, and so it can run on a machine with no GPU.
    Rows produced this way are flagged `no_corrector`, and they are NOT part
    of the publishable dataset -- a pair whose two sides are identical teaches
    nothing.
    """

    name = "passthrough"

    def read(self, image, raw_text: str = "") -> tuple:
        return raw_text, 0.0


class LightOnCorrector:
    """
    The fine-tuned LightOnOCR that supplies the corrected column.

    Loading is deliberately lazy and explicit about precision, because the
    same overflow problem that shaped train_lighton_ocr.py applies here: this
    model's forward pass returns nan in fp16 on a Turing card, and generation
    is the same forward pass. Getting that wrong produces garbage text rather
    than an error, which is the worst possible failure for a data pipeline.

    `adapter_dir` is the LoRA folder produced by train_lighton_ocr.py. Passing
    None loads the untuned base model, which is useful for measuring what the
    fine-tune actually bought but is NOT what the thesis ships -- the PP1
    panel asked for a fine-tuned model.
    """

    def __init__(
        self,
        adapter_dir: str = None,
        model_name: str = "lightonai/LightOnOCR-2-1B",
        max_image_edge: int = 1536,
        max_new_tokens: int = 4608,
        precision: str = "auto",
    ):
        import torch
        from PIL import Image
        from transformers import AutoProcessor, LightOnOcrForConditionalGeneration

        self._torch = torch
        self._Image = Image
        self.max_image_edge = max_image_edge
        self.max_new_tokens = max_new_tokens
        self.adapter_dir = adapter_dir
        self.name = "lighton-base" if adapter_dir is None else "lighton-finetuned"

        if precision == "auto":
            if not torch.cuda.is_available():
                precision = "fp32"
            elif torch.cuda.is_bf16_supported():
                precision = "bf16"
            else:
                # Turing and older. fp16 overflows in BOTH halves of this
                # model -- measured, see train_lighton_ocr.py -- so fp32 is
                # the only safe option even though it is several times slower.
                precision = "fp32"
        self.precision = precision
        dtype = {"bf16": torch.bfloat16, "fp16": torch.float16,
                 "fp32": torch.float32}[precision]

        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"[corrector] loading {model_name} ({precision}) on {device}")
        if device == "cpu":
            print("[corrector] WARNING: no GPU. Expect minutes per page, not "
                  "seconds. Fine for a smoke test, unusable for a real run.")

        model = LightOnOcrForConditionalGeneration.from_pretrained(
            model_name, dtype=dtype,
            device_map={"": 0} if device == "cuda" else None,
        )
        if adapter_dir:
            from peft import PeftModel
            print(f"[corrector] merging adapter from {adapter_dir}")
            model = PeftModel.from_pretrained(model, adapter_dir).merge_and_unload()
        if device == "cpu":
            model = model.to("cpu")

        model.eval()
        model.config.use_cache = True
        self.model = model
        self.processor = AutoProcessor.from_pretrained(model_name)
        self._prompt_messages = [{"role": "user", "content": [{"type": "image"}]}]

    def _shrink(self, image):
        """Cap the longer edge. Same rule the model was fine-tuned under."""
        if image.mode != "RGB":
            image = image.convert("RGB")
        w, h = image.size
        longest = max(w, h)
        if longest <= self.max_image_edge:
            return image
        scale = self.max_image_edge / longest
        return image.resize((int(w * scale), int(h * scale)),
                            self._Image.LANCZOS)

    def read(self, image, raw_text: str = "") -> tuple:
        """
        Return (text, seconds).

        `hit_cap` is recorded on the instance rather than returned, so callers
        that do not care are not forced to unpack it. generate.py reads it to
        flag pages where the model ran out of room mid-page -- those rows have
        a truncated corrected side and must never be published as if complete.
        """
        torch = self._torch
        prompt = self.processor.apply_chat_template(
            self._prompt_messages, add_generation_prompt=True, tokenize=False
        )
        inputs = self.processor(
            images=[self._shrink(image)], text=[prompt], return_tensors="pt"
        ).to(self.model.device)

        t0 = time.time()
        with torch.no_grad():
            out = self.model.generate(
                **inputs,
                max_new_tokens=self.max_new_tokens,
                do_sample=False,        # greedy: reading a page is not creative
            )
        elapsed = time.time() - t0

        generated = out[0, inputs["input_ids"].shape[1]:]
        self.hit_cap = len(generated) >= self.max_new_tokens
        text = self.processor.decode(generated, skip_special_tokens=True)
        return normalize(text), elapsed


def build_corrector(kind: str, adapter_dir=None, **kwargs):
    """Factory so generate.py does not import torch unless it has to."""
    if kind == "none":
        return PassthroughCorrector()
    if kind == "lighton":
        return LightOnCorrector(adapter_dir=adapter_dir, **kwargs)
    raise ValueError(f"unknown corrector {kind!r}; use 'none' or 'lighton'")
