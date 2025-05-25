import fasttext
from functools import lru_cache

fasttext_model_path = "/data/classifiers/lid.176.bin"

@lru_cache
def fasttext_model():
    return fasttext.load_model(fasttext_model_path)

def identify_language(text: str):
    cleaned_text = text.replace("\n", " ")
    model = fasttext_model()
    predictions = model.predict(cleaned_text)

    cleaned_predictions = (predictions[0][0].replace("__label__", ""), predictions[1][0])
    return cleaned_predictions