import nltk
nltk.download('punkt_tab')
import re
import numpy as np

def check_gopher_filters(text: str):
    words = nltk.tokenize.word_tokenize(text)

    if len(words) < 50 or len(words) >= 100_000:
        return False

    word_lengths = []
    num_words_alphabetic = 0
    for word in words:
        word_lengths.append(len(word))

        if re.search(r'[a-zA-Z]', word):
            num_words_alphabetic += 1
    
    mean_word_length = np.mean(word_lengths)
    
    if mean_word_length < 3 or mean_word_length > 10:
        return False

    if num_words_alphabetic / len(words) < 0.80:
        return False
    
    line_delimited_text = text.split('\n')
    num_lines = len(line_delimited_text)
    num_lines_end_ellipsis = 0
    for line in line_delimited_text:
        if line.endswith("..."):
            num_lines_end_ellipsis += 1
        
    if num_lines_end_ellipsis / num_lines > 0.30:
        return False
    
        
    return True