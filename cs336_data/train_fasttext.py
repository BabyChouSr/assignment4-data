import fasttext
import draccus
from dataclasses import dataclass

@dataclass
class TrainFasttextConfig:
    input_path: str
    output_path: str

@draccus.wrap()
def main(config: TrainFasttextConfig):
    model = fasttext.train_supervised(input=config.input_path, wordNgrams=2)
    model.save_model(config.output_path)

if __name__ == "__main__":
    main()