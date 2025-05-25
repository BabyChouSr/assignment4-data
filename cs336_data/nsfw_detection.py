import fasttext
from functools import lru_cache

hatespeech_model_path = "/data/classifiers/dolma_fasttext_hatespeech_jigsaw_model.bin"
nsfw_model_path = "/data/classifiers/dolma_fasttext_nsfw_jigsaw_model.bin"

@lru_cache
def hatespeech_model():
    return fasttext.load_model(hatespeech_model_path)

@lru_cache
def nsfw_model():
    return fasttext.load_model(nsfw_model_path)

harm_type_to_model_path = {
    "nsfw": nsfw_model,
    "hate": hatespeech_model
}

def classify(text: str, harm_type: str):
    model = harm_type_to_model_path[harm_type]()
    prediction = model.predict(text.replace("\n", " "))

    cleaned_prediction = (prediction[0][0].replace("__label__", ""), prediction[1][0])
    return cleaned_prediction