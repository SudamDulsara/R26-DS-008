def clean_text(text):
    text = text.strip()

    # remove unwanted characters
    text = text.replace("\n", " ")

    return text