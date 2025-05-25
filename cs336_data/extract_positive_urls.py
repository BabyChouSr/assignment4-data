import fsspec

POSITIVE_SEED_SET = "/data/wiki/enwiki-20240420-extracted_urls.txt.gz"
OUTPUT_PATH = "subsampled_positive_urls_100k.txt"
NUM_URLS = 100000

with open(OUTPUT_PATH, "w") as f:
    with fsspec.open(POSITIVE_SEED_SET, "rt", compression="gzip") as g:
        for i, line in enumerate(g):
            if i >= NUM_URLS:
                break
            url = line.strip()
            if url:
                f.write(url + "\n")

            

