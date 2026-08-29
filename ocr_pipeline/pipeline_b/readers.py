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

    #: Where the Windows installer puts tesseract.exe.
    #:
    #: pytesseract shells out to `tesseract` and therefore needs it on PATH.
    #: The UB Mannheim installer offers to add it, the box is easy to miss,
    #: and a terminal opened before the install will not see it regardless --
    #: so on a fresh machine the pipeline fails at the first page with
    #: "Tesseract is not available" even though it is installed.
    #:
    #: pipeline/ocr_engine.py has hunted these paths since the Streamlit demo
    #: was written. Doing the same here means setting this up on a colleague's
    #: laptop does not turn into a PATH debugging session.
    WINDOWS_PATHS = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    )

    def __init__(self, lang: str = "sin"):
        import pytesseract
        self._pt = pytesseract
        self.lang = lang

        if os.name == "nt":
            user_path = os.path.join(
                os.environ.get("LOCALAPPDATA", ""),
                "Programs", "Tesseract-OCR", "tesseract.exe",
            )
            for path in self.WINDOWS_PATHS + (user_path,):
                if path and os.path.exists(path):
                    pytesseract.pytesseract.tesseract_cmd = path
                    break

        try:
            self.version = str(pytesseract.get_tesseract_version())
        except Exception as exc:                    # not installed / not found
            raise RuntimeError(
                "Tesseract is not available. Install it system-wide with the "
                "'sin' language pack (the UB Mannheim installer for Windows), "
                "then either tick 'Add to PATH' during setup or open a NEW "
                "terminal afterwards. Looked in: PATH, "
                + ", ".join(self.WINDOWS_PATHS)
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
            # NOT torch.cuda.is_bf16_supported(). That returns True whenever
            # bf16 can be EMULATED, and it duly said yes on a Tesla T4, which
            # is Turing and has no bf16 units -- giving none of the speed and
            # a dtype the card does not really have. train_lighton_ocr.py hit
            # this and replaced it with the same compute-capability test used
            # here: hardware bf16 starts at 8.0 (Ampere). Turing is 7.5,
            # Ada 8.9, Blackwell 12.x.
            if not torch.cuda.is_available():
                precision = "fp32"
            elif torch.cuda.get_device_capability()[0] >= 8:
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


class ByT5Corrector:
    """
    The ByT5 trained on this pipeline's own output, used as the corrector.

    This one is different in kind from LightOnCorrector, and the difference
    matters: LightOnOCR reads the IMAGE again, ByT5 repairs the TEXT. It never
    looks at the page. That makes it far cheaper -- no vision encoder, ~300M
    parameters, and it runs on a CPU if it has to -- and it is the model this
    project actually contributed, trained on pairs this pipeline generated.

    Measured on the 202 human-transcribed acts-1010 test pages: Tesseract
    alone 0.1079 CER, Tesseract + this model 0.0891. 17.4% fewer character
    errors. 156 of 202 pages improved, 46 got worse.

    LINE BY LINE, because that is how it was trained. The training pairs are
    aligned lines, not pages, so feeding it a whole page asks it for something
    it has never seen. The page is split on newlines, each line is corrected
    on its own, and the page is put back together with the same line breaks.
    Blank lines are preserved untouched and never sent to the model.

    OVERLONG LINES ARE PASSED THROUGH, NOT TRUNCATED. ByT5 counts BYTES, and
    Sinhala costs about 3 bytes per character, so the 384-byte window holds
    roughly 128 characters. Anything longer would be silently cut in half and
    the tail lost -- which in a published dataset is invisible and permanent.
    The project rule is skip, split or flag, never truncate quietly, so such
    lines are returned exactly as Tesseract read them and counted in
    `lines_too_long`.
    """

    name = "byt5"

    def __init__(
        self,
        model_dir: str,
        max_length: int = 384,
        batch_size: int = 8,
        precision: str = "auto",
        **_ignored,                 # generate.py passes max_image_edge
    ):
        import torch
        from transformers import AutoTokenizer, T5ForConditionalGeneration

        self._torch = torch
        self.max_length = max_length
        self.batch_size = batch_size
        self.model_dir = model_dir

        # Counters for the run summary. lines_too_long is the one to watch:
        # if it is large the window is wrong for this corpus, and on acts-1010
        # nothing exceeded 384 bytes, so it should be near zero.
        self.lines_seen = 0
        self.lines_too_long = 0
        self.lines_changed = 0

        if precision == "auto":
            # Same reasoning as LightOnCorrector, plus one of this project's
            # own scars: T5-family models are pretrained in bfloat16 and their
            # activations overflow fp16's exponent range. A fp16 ByT5 run here
            # reached training loss 14,344,448 and nan. fp16 is never correct
            # for this model, on any card.
            if not torch.cuda.is_available():
                precision = "fp32"
            elif torch.cuda.get_device_capability()[0] >= 8:
                precision = "bf16"
            else:
                precision = "fp32"
        self.precision = precision
        dtype = {"bf16": torch.bfloat16, "fp32": torch.float32}[precision]

        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"[corrector] loading ByT5 from {model_dir} ({precision}) on {device}")

        self.tokenizer = AutoTokenizer.from_pretrained(model_dir)
        model = T5ForConditionalGeneration.from_pretrained(model_dir, dtype=dtype)
        model = model.to(device)
        model.eval()
        model.config.use_cache = True
        self.model = model
        self.device = device

    def _correct_batch(self, lines):
        torch = self._torch
        enc = self.tokenizer(
            lines, return_tensors="pt", padding=True,
            truncation=True, max_length=self.max_length,
        ).to(self.device)
        with torch.no_grad():
            out = self.model.generate(
                **enc,
                max_length=self.max_length,
                num_beams=1,          # greedy: repairing a line is not creative
            )
        return self.tokenizer.batch_decode(out, skip_special_tokens=True)

    def read(self, image, raw_text: str = "") -> tuple:
        """
        Return (text, seconds). `image` is accepted and ignored -- this
        corrector works from Tesseract's text, which is the whole point of it.
        """
        t0 = time.time()
        lines = raw_text.split("\n")

        # Which lines actually go to the model. Blank lines keep their place
        # in the page; overlong ones are passed through and counted.
        todo = []
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            self.lines_seen += 1
            if len(line.encode("utf-8")) > self.max_length:
                self.lines_too_long += 1
                continue
            todo.append(i)

        out = list(lines)
        for start in range(0, len(todo), self.batch_size):
            idxs = todo[start:start + self.batch_size]
            fixed = self._correct_batch([lines[i] for i in idxs])
            for i, new in zip(idxs, fixed):
                if new != lines[i]:
                    self.lines_changed += 1
                out[i] = new

        # LightOnCorrector sets this because it generates a whole page and can
        # run out of room. Here every line is capped independently and long
        # lines are passed through instead, so the page is never a fragment.
        self.hit_cap = False
        return normalize("\n".join(out)), time.time() - t0


def build_corrector(kind: str, adapter_dir=None, model_dir=None, **kwargs):
    """Factory so generate.py does not import torch unless it has to."""
    if kind == "none":
        return PassthroughCorrector()
    if kind == "lighton":
        return LightOnCorrector(adapter_dir=adapter_dir, **kwargs)
    if kind == "byt5":
        target = model_dir or adapter_dir
        if not target:
            raise ValueError(
                "--corrector byt5 needs --model pointing at the trained ByT5 "
                "folder (the one holding config.json and model.safetensors)"
            )
        return ByT5Corrector(model_dir=target, **kwargs)
    raise ValueError(
        f"unknown corrector {kind!r}; use 'none', 'lighton' or 'byt5'"
    )
