import sys
sys.path.append(r"E:\streamlit_packages")

import streamlit as st
import pandas as pd
import os

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="Sinhala ASR Pipeline",
    layout="wide"
)

# =====================================================
# TITLE
# =====================================================

st.title("🎤 Sinhala Audio-Text Dataset Pipeline")

st.markdown("""
This project demonstrates a continuous Sinhala audio-text dataset generation pipeline using:
- YouTube audio extraction
- Audio preprocessing
- Sinhala Whisper ASR
- Rule-based correction
- WER/CER evaluation
""")

# =====================================================
# PIPELINE OVERVIEW
# =====================================================

st.header("📌 Pipeline Architecture")

st.code("""
YouTube Video
      ↓
Audio Download
      ↓
Noise Filtering
      ↓
Audio Segmentation
      ↓
Whisper Sinhala ASR
      ↓
Rule-Based Correction
      ↓
WER/CER Evaluation
      ↓
Dataset Storage
""")

# =====================================================
# DATASET SECTION
# =====================================================

st.header("📂 Generated Dataset")

dataset_path = "dataset/train.tsv"

if os.path.exists(dataset_path):

    try:

        df = pd.read_csv(
            dataset_path,
            sep="\t"
        )

        st.success(f"Loaded {len(df)} rows")

        st.dataframe(df)

    except Exception as e:

        st.error(f"Error loading dataset: {e}")

else:

    st.warning("dataset/train.tsv not found")

# =====================================================
# EVALUATION RESULTS
# =====================================================

st.header("📊 Evaluation Metrics")

col1, col2 = st.columns(2)

with col1:

    st.metric(
        label="Average WER",
        value="25.83%"
    )

with col2:

    st.metric(
        label="Average CER",
        value="7.38%"
    )

# =====================================================
# SAMPLE TRANSCRIPTIONS
# =====================================================

st.header("📝 Sample Predictions")

samples = [

    {
        "Reference": "අද අපි ඔයාට කියන්න යන්නේ උකුස්සා සහ ඊතලය කියන කතාව",
        "Prediction": "අදාපියෝාට කියන්න යන්නේ උකුස්සා සහ ඊ තළේ කියන කතාව"
    },

    {
        "Reference": "ඉතාමත්ම වේගයෙන් ඉහළට ගියපු මේ ඊතලය උකුස්සාගේ පපුව පසාරු කරගෙන ගිහිල්ලා",
        "Prediction": "ඉතාමත්ම වේගෙන් ඉහළට ගියපු මේ ඊතලය උකුස්සගෙ පපුව පසාරු කරගෙන ගිහිල්ල"
    },

    {
        "Reference": "මේ ඇදගෙන වැටුණු ඊතලයේ උල් පැත්ත පොළොවට ඇනිලා",
        "Prediction": "මේ ඇදගෙන වැටුණු ඊතලයේ උල් පැත්ත පොළොවට ඇනිලා"
    }
]

for i, sample in enumerate(samples):

    with st.expander(f"Sample {i+1}"):

        st.write("### ✅ Reference")
        st.write(sample["Reference"])

        st.write("### 🎤 Prediction")
        st.write(sample["Prediction"])

# =====================================================
# PROJECT COMPONENTS
# =====================================================

st.header("⚙ Implemented Components")

st.markdown("""
### Completed Modules

✅ YouTube Audio Extraction  
✅ Audio Preprocessing  
✅ Noise Filtering  
✅ Audio Segmentation  
✅ Whisper Sinhala Transcription  
✅ Rule-Based Correction  
✅ Dataset Generation  
✅ WER/CER Evaluation  
✅ Continuous Pipeline Automation
""")

# =====================================================
# FINAL SUMMARY
# =====================================================

st.header("🚀 Final Summary")

st.success(
    "Continuous Sinhala Audio-Text Dataset Pipeline Successfully Implemented"
)

st.markdown("""
This system automatically:
- downloads Sinhala content,
- preprocesses audio,
- transcribes speech,
- applies correction,
- evaluates ASR quality,
- and stores audio-text pairs for future model training.
""")