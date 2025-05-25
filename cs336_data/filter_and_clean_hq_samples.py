from cs336_data.gopher import check_gopher_filters
from cs336_data.language_identification import identify_language
from cs336_data.nsfw_detection import classify
from dataclasses import dataclass
import draccus


@dataclass
class CleanSamplesConfig:
    input_path: str
    output_path: str


@draccus.wrap()
def main(config: CleanSamplesConfig):

    num_total_samples = 0
    num_filtered_samples = 0
    with open(config.input_path, "r") as f_in:
        with open(config.output_path, "w") as f_out:
            for i, line in enumerate(f_in):
                num_total_samples += 1
                label_plus_text = line.strip().split(" ", 1)
                if len(label_plus_text) == 1:
                    continue

                label, text = label_plus_text
                language, _ = identify_language(text)
                if language != "en":
                    continue
                
                toxic_flag, _ = classify(text, "hate")
                nsfw_flag, _ = classify(text, "nsfw")
                if toxic_flag == "toxic" or nsfw_flag == "nsfw":
                    continue
                
                gopher_flag_to_keep = check_gopher_filters(text)
                if not gopher_flag_to_keep:
                    continue
                
                f_out.write(line)
                num_filtered_samples += 1

    print(f"Total samples: {num_total_samples}")
    print(f"Filtered samples: {num_filtered_samples}")
    print(f"Yield: {num_filtered_samples / num_total_samples}")

if __name__ == "__main__":
    main()