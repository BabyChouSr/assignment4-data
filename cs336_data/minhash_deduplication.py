import os
import string
import re
import unicodedata
import mmh3

def remove_accents(text: str) -> str:
    return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if not unicodedata.combining(c)
        )

def normalize_text(text: str) -> str:
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\s+', ' ', text)
    text = remove_accents(text)
    return text

def get_ngrams(text: str, n: int) -> list[str]:
    tokens = text.split()
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]

def get_char_ngrams(text: str, n: int) -> list[str]:
    """Generate character-level n-grams, which work better for fuzzy duplicate detection"""
    return [text[i:i+n] for i in range(len(text) - n + 1)]
    
def get_min_hash_signature(text: str, num_hashes: int, ngrams: int, use_char_ngrams=True) -> list[int]:
    # For fuzzy duplicates, character-level n-grams often work better
    if use_char_ngrams:
        shingles = get_char_ngrams(text, ngrams)
    else:
        shingles = get_ngrams(text, ngrams)
        
    if not shingles:
        return [0] * num_hashes  # Return zeros if no shingles found
    
    min_hash_signature = []
    for hash_num in range(num_hashes):
        hashed_shingles = [mmh3.hash(shingle, seed=hash_num) for shingle in shingles]
        min_hash_signature.append(min(hashed_shingles))
    return min_hash_signature

def lsh_bands(signature, num_bands):
    rows_per_band = len(signature) // num_bands
    return [tuple(signature[i:i+rows_per_band]) for i in range(0, len(signature), rows_per_band)]

def estimate_jaccard_similarity(sig1, sig2):
    """Estimate Jaccard similarity between two MinHash signatures"""
    if not sig1 or not sig2:
        return 0.0
    matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
    return matches / len(sig1)

def minhash_deduplication(
    input_files: list[os.PathLike],
    num_hashes: int,
    num_bands: int,
    ngrams: int,
    jaccard_threshold: float,
    output_directory: os.PathLike,
    use_char_ngrams=True,  # Default to character-level n-grams for fuzzy duplicates
):
    os.makedirs(output_directory, exist_ok=True)

    # Process all input files and compute signatures
    signatures = []
    original_texts = []
        
    for file_idx, input_file in enumerate(input_files):
        with open(input_file, 'r') as f:
            original_text = f.read()
            original_texts.append(original_text)
            text = normalize_text(original_text)
            min_hash_signature = get_min_hash_signature(text, num_hashes, ngrams, use_char_ngrams)
            signatures.append((file_idx, min_hash_signature))
    
    # Find candidate pairs using LSH
    buckets = {}
    candidate_pairs = set()
    
    for file_idx, signature in enumerate(signatures):
        for band_idx, band in enumerate(lsh_bands(signature[1], num_bands)):
            key = (band_idx, band)
            if key in buckets:
                # Add candidate pairs
                for other_idx in buckets[key]:
                    candidate_pairs.add((min(file_idx, other_idx), max(file_idx, other_idx)))
                buckets[key].append(file_idx)
            else:
                buckets[key] = [file_idx]
    
    # Determine duplicates by calculating actual Jaccard similarity
    duplicates = set()
    for idx1, idx2 in candidate_pairs:
        similarity = estimate_jaccard_similarity(signatures[idx1][1], signatures[idx2][1])
        if similarity >= jaccard_threshold:
            duplicates.add(idx2)  # Keep idx1 (the smaller index) as the original
            
    # Write non-duplicate documents
    for file_idx, input_file in enumerate(input_files):
        if file_idx not in duplicates:
            output_file_path = os.path.join(output_directory, os.path.basename(input_file))
            with open(output_file_path, 'w') as f_out:
                f_out.write(original_texts[file_idx])

            
                                
                

    