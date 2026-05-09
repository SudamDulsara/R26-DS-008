"""
OCR Engine Module
==================
Wraps Tesseract OCR to extract raw Sinhala text from uploaded images.

This module sits BEFORE the 4-stage refinement pipeline:

  Image Upload
       ↓
  [OCR Engine]  ← THIS MODULE
       ↓
  Raw Noisy Sinhala Text
       ↓
  Stage 1 → Stage 2 → Stage 3 → Stage 4
       ↓
  High-Fidelity Sinhala Text

Tesseract is used because:
  - Free and open source
  - Supports Sinhala script (sin language pack)
  - Widely used in Sinhala OCR research benchmarks
  - Referenced in the project proposal literature review

Installation required on your machine:
  Windows: https://github.com/UB-Mannheim/tesseract/wiki
           During install tick "Sinhala" under Additional scripts
  Mac:     brew install tesseract tesseract-lang
  Linux:   sudo apt install tesseract-ocr tesseract-ocr-sin
"""

import io
import platform
from PIL import Image

# ─────────────────────────────────────────────────────────────
#  Tesseract path setup (Windows needs explicit path)
# ─────────────────────────────────────────────────────────────

try:
    import pytesseract

    # Common Windows install paths — adjust if yours differs
    if platform.system() == "Windows":
        import os
        possible_paths = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            r"C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe".format(
                os.environ.get("USERNAME", "user")
            ),
        ]
        for path in possible_paths:
            if os.path.exists(path):
                pytesseract.pytesseract.tesseract_cmd = path
                break

    TESSERACT_AVAILABLE = True

except ImportError:
    TESSERACT_AVAILABLE = False


# ─────────────────────────────────────────────────────────────
#  Check Sinhala language pack availability
# ─────────────────────────────────────────────────────────────

def check_sinhala_available() -> bool:
    """Check if Tesseract Sinhala language pack is installed."""
    if not TESSERACT_AVAILABLE:
        return False
    try:
        langs = pytesseract.get_languages()
        return "sin" in langs
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
#  Core OCR function
# ─────────────────────────────────────────────────────────────

def run_ocr(image_bytes: bytes, language: str = "sin") -> dict:
    """
    Run Tesseract OCR on image bytes.

    Parameters:
        image_bytes : raw bytes of uploaded image (jpg/png/etc.)
        language    : Tesseract language code
                      'sin'     = Sinhala only
                      'sin+eng' = Sinhala + English mixed

    Returns a dict:
        'raw_text'      : extracted text string
        'success'       : bool
        'error'         : error message if failed
        'language_used' : language code actually used
        'engine'        : engine name for display
    """
    if not TESSERACT_AVAILABLE:
        return {
            "raw_text": "",
            "success": False,
            "error": "pytesseract not installed. Run: pip install pytesseract",
            "language_used": language,
            "engine": "Tesseract",
        }

    try:
        # Open image from bytes
        image = Image.open(io.BytesIO(image_bytes))

        # Convert to RGB if needed (handles PNG with alpha channel)
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")

        # Check if Sinhala pack available — fall back to eng if not
        sinhala_ok = check_sinhala_available()
        lang_to_use = language if sinhala_ok else "eng"

        # Tesseract config:
        # --oem 3  = default LSTM engine (best accuracy)
        # --psm 3  = fully automatic page segmentation
        custom_config = r"--oem 3 --psm 3"

        raw_text = pytesseract.image_to_string(
            image,
            lang=lang_to_use,
            config=custom_config,
        )

        return {
            "raw_text": raw_text.strip(),
            "success": True,
            "error": None,
            "language_used": lang_to_use,
            "engine": "Tesseract OCR",
            "sinhala_pack": sinhala_ok,
        }

    except pytesseract.TesseractNotFoundError:
        return {
            "raw_text": "",
            "success": False,
            "error": (
                "Tesseract is not installed or not found on your system.\n\n"
                "Install it from: https://github.com/UB-Mannheim/tesseract/wiki\n"
                "During installation, tick 'Sinhala' under Additional Scripts."
            ),
            "language_used": language,
            "engine": "Tesseract OCR",
        }

    except Exception as e:
        return {
            "raw_text": "",
            "success": False,
            "error": str(e),
            "language_used": language,
            "engine": "Tesseract OCR",
        }


# ─────────────────────────────────────────────────────────────
#  Installation guide helper
# ─────────────────────────────────────────────────────────────

INSTALL_GUIDE = """
**Tesseract is not installed or Sinhala language pack is missing.**

### Windows Installation Steps:
1. Download Tesseract installer from:
   https://github.com/UB-Mannheim/tesseract/wiki
2. Run the installer
3. On the **"Choose Components"** screen:
   - Expand **"Additional script data"**
   - Tick ✅ **Sinhala Script**
   - Tick ✅ **Sinhala**
4. Complete installation
5. Restart VS Code / terminal
6. Run: `pip install pytesseract pillow`

### Verify installation:
Open a new terminal and run:
```
tesseract --list-langs
```
You should see `sin` in the list.
"""