# R26-DS-008

Pipelines for building Sinhala datasets: news articles, OCR, speech-to-text (ASR), and a quality-check dashboard.

## 1. Install these first

- [Python 3.12 or 3.13](https://www.python.org/downloads/)
- [FFmpeg](https://ffmpeg.org/download.html), added to your PATH (audio pipeline)
- [Tesseract OCR](https://tesseract-ocr.github.io/tessdoc/Installation.html) with the **Sinhala** language (OCR pipeline). On Windows, tick *Sinhala* under "Additional language data" in the installer.
- [Node.js](https://nodejs.org/) (audio pipeline, used to download YouTube audio)

## 2. Set up

```bash
git clone https://github.com/SudamDulsara/R26-DS-008.git
cd R26-DS-008
python -m venv .venv
.venv\Scripts\activate          # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

**Have an NVIDIA GPU?** Run this *before* `pip install -r requirements.txt` so PyTorch can use it:

```bash
pip install torch==2.14.1 torchvision==0.29.1 --index-url https://download.pytorch.org/whl/cu126
```

## 3. Add your API keys

Copy `.env.example` to `.env`, then fill in the keys you need:

```bash
copy .env.example .env          # Mac/Linux: cp .env.example .env
```

| Key | Needed for |
|---|---|
| `OPENAI_API_KEY` | News pipeline (not needed with `--no-gpt`) |
| `YOUTUBE_API_KEY` | Audio pipeline |
| `OPENROUTER_API_KEY` | OCR app, Stage 2 (optional) |

## 4. Get the models

The trained models are too big for Git. Ask the team for these folders and put them here:

- `ocr_pipeline/models/byt5-sinhala-ocr-v2`
- `audio-text-project_1/models/final_whisper_sinhala_v2`

## 5. Run

Start from the project folder with the virtual environment active.

| What | Command |
|---|---|
| News pipeline | `python main.py` (no OpenAI key: `python main.py run --no-gpt`) |
| OCR pipeline | `cd ocr_pipeline` then `python -m pipeline_b.generate --corrector byt5 --model models\byt5-sinhala-ocr-v2 --input data\unseen_acts --fetch 4 --cycles 1` |
| OCR app | `cd ocr_pipeline` then `streamlit run app.py` |
| Audio pipeline | `cd audio-text-project_1` then `python main.py` |
| Quality dashboard | `streamlit run quality_pipeline/frontend/app.py` |
