# R26-DS-008

Pipelines for building Sinhala datasets: news articles, OCR, speech-to-text (ASR), and a quality-check dashboard.

| Component | Folder | What it does |
|---|---|---|
| News pipeline | `news_pipeline/` | Crawls Sinhala news RSS feeds, cleans and deduplicates articles, clusters same-event stories and merges them with GPT |
| OCR pipeline | `ocr_pipeline/` | Downloads scanned Acts from documents.gov.lk, runs Tesseract OCR and corrects the output with a fine-tuned ByT5 model |
| Audio pipeline | `audio-text-project_1/` | Finds Sinhala YouTube videos, downloads and splits the audio, and transcribes it with a fine-tuned Whisper model |
| Quality pipeline | `quality_pipeline/` | Checks the output of all three (Unicode normalization, duplicates, morphology score, semantic overlap) and shows it in a dashboard |

## 1. Install these first

- [Python 3.12 or 3.13](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)
- [FFmpeg](https://ffmpeg.org/download.html), added to your PATH (audio pipeline). Check with `ffmpeg -version`.
- [Tesseract OCR](https://tesseract-ocr.github.io/tessdoc/Installation.html) with the **Sinhala** language (OCR pipeline). On Windows, tick *Sinhala* under "Additional language data" in the installer. Check with `tesseract --list-langs`, which should list `sin`.
- [Node.js](https://nodejs.org/) (audio pipeline, used by yt-dlp to download YouTube audio). Check with `node --version`.
- Optional: an NVIDIA GPU. Everything runs on CPU, but Whisper transcription and OCR correction are much slower without one.

## 2. Set up

```bash
git clone https://github.com/SudamDulsara/R26-DS-008.git
cd R26-DS-008
python -m venv .venv
.venv\Scripts\activate          # Mac/Linux: source .venv/bin/activate
python -m pip install --upgrade pip
```

**Have an NVIDIA GPU?** Install the CUDA build of PyTorch *before* the rest, so PyTorch can use it. The versions must match the ones pinned in `requirements.txt`:

```bash
pip install torch==2.11.0 torchvision==0.26.0 torchaudio==2.11.0 --index-url https://download.pytorch.org/whl/cu126
```

Then install everything else:

```bash
pip install -r requirements.txt
```

One `requirements.txt` at the project root covers all four components. Do not install `ocr_pipeline/requirements.txt` into the same environment: it is the OCR project's original standalone list and pins older versions of streamlit, pandas and openai.

## 3. Add your API keys

Copy `.env.example` to `.env`, then fill in the keys you need:

```bash
copy .env.example .env          # Mac/Linux: cp .env.example .env
```

| Key | Needed for | Where to get it |
|---|---|---|
| `OPENAI_API_KEY` | News pipeline (not needed with `--no-gpt`) | [platform.openai.com](https://platform.openai.com/api-keys) |
| `YOUTUBE_API_KEY` | Audio pipeline | [Google Cloud Console](https://console.cloud.google.com/apis/credentials), with "YouTube Data API v3" enabled |
| `OPENROUTER_API_KEY` | OCR app, Stage 2 (optional) | [openrouter.ai](https://openrouter.ai/keys) |
| `HF_TOKEN` | Optional, for Hugging Face downloads | [huggingface.co](https://huggingface.co/settings/tokens) |

The `NEWS_PIPELINE_*` lines in `.env.example` are working defaults for the news pipeline, so you can leave them as they are. They include spending limits for GPT calls (by default $1 per run, $2 per day and $20 per month).

Never commit `.env`. It is already in `.gitignore`.

## 4. Get the models

The trained models are too big for Git. Ask the team for these folders and put them here:

- `ocr_pipeline/models/byt5-sinhala-ocr-v2`
- `audio-text-project_1/models/final_whisper_sinhala_v2` (it must contain the `checkpoint-72` folder)

Two more models download from Hugging Face by themselves the first time you run, so the first run needs internet and takes longer:

- `intfloat/multilingual-e5-base` (news pipeline, story clustering)
- `sentence-transformers/LaBSE` (quality pipeline, semantic overlap)

## 5. Run

Start from the project folder with the virtual environment active.

| What | Command |
|---|---|
| News pipeline | `python main.py` (no OpenAI key: `python main.py run --no-gpt`) |
| OCR pipeline | `cd ocr_pipeline` then `python -m pipeline_b.generate --corrector byt5 --model models\byt5-sinhala-ocr-v2 --input data\unseen_acts --fetch 4 --cycles 1` |
| OCR app | `cd ocr_pipeline` then `streamlit run app.py` |
| Audio pipeline | `cd audio-text-project_1` then `python main.py` |
| Quality dashboard | `streamlit run quality_pipeline/frontend/app.py` |
| Quality pipeline (no dashboard) | `python -m quality_pipeline.integration.run_integration` |

More detail on each:

- **News pipeline.** `python main.py` runs the full pipeline. `python main.py status` shows the health of the last runs, and `python main.py --help` lists the single steps (`discover`, `extract`, `clean`, `dedupe`, `cluster`, `unify`, `export` and more). The RSS feeds are set in `news_pipeline/pipeline_config.json` under `news_sources`.
- **OCR pipeline.** `--fetch 4` downloads up to 4 new Acts into `ocr_pipeline/data/unseen_acts`, and `--cycles 1` stops after one pass. Leave out `--cycles` to keep it running every 30 minutes until you press Ctrl+C. Add `--status` to see what has already been processed.
- **Audio pipeline.** Each run processes one new video. The YouTube search terms and channels are in `audio-text-project_1/config.py`. Always run it from inside `audio-text-project_1`, because its paths are relative to that folder.
- **Quality dashboard.** The *Continuous Pipeline* page can run the three pipelines above one after another and then run the quality checks on everything new. The OCR step runs in offline mode there, so the ByT5 model from step 4 must be in place.

## 6. Where the output goes

All data folders are in `.gitignore`, so they only exist on your computer.

| Folder | Contents |
|---|---|
| `data/inbox/` | `news_pipeline.db`, `ocr_correction_pairs.db` and `videos.db`, one per pipeline |
| `data/outbox/` | Quality results: `verdicts.db`, `training_corpus.jsonl` (accepted documents only) and a `pipeline_run_<time>.xlsx` report per run |
| `data/snapshots/` | Versioned exports of the news dataset |
| `audio-text-project_1/dataset/clips/` | Audio clips cut from each video |
| `logs/` | News pipeline logs |

Note that `ocr_pipeline/data/inbox/` is a different folder. It holds scanned PDFs waiting to be read, not results.

## 7. Tests

```bash
python -m quality_pipeline.test_pipeline                         # quality pipeline smoke test on sample data
python -m unittest news_pipeline.tests.test_autonomous_pipeline  # news pipeline unit tests
```

The quality smoke test writes its report to `results/pipeline_run.xlsx`.

## 8. Troubleshooting

| Problem | Fix |
|---|---|
| `.venv\Scripts\activate` is blocked in PowerShell | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once, then try again |
| "Tesseract is not available" | Install Tesseract to the default folder (`C:\Program Files\Tesseract-OCR`), or add it to your PATH. Make sure the Sinhala language is installed. |
| Audio download fails or YouTube returns errors | Update yt-dlp with `pip install -U yt-dlp`, and check that `node --version` works |
| `ffmpeg` or `ffprobe` not found | Add the FFmpeg `bin` folder to your PATH, then open a new terminal |
| `UnicodeEncodeError` or broken Sinhala text in the terminal | Run `$env:PYTHONIOENCODING="utf-8"` in PowerShell (cmd: `set PYTHONIOENCODING=utf-8`) before the command |
| PyTorch does not use the GPU | Check with `python -c "import torch; print(torch.cuda.is_available())"`. If it prints `False`, repeat the GPU install command from step 2. |
