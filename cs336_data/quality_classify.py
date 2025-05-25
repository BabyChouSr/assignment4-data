import fasttext

model = fasttext.load_model("fasttext_model.bin")

def classify_quality(text: str) -> str:
    prediction = model.predict(text.replace("\n", " "))

    cleaned_prediction = (prediction[0][0].replace("__label__", ""), prediction[1][0])
    return cleaned_prediction
