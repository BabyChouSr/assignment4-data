from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import detect_encoding

def extract_text_from_html_bytes(html_bytes: bytes | str):
    if isinstance(html_bytes, str):
        return html_bytes

    try:
        enc = detect_encoding(html_bytes)
        html_str = html_bytes.decode(enc, errors='replace')
    except Exception as e:
        raise Exception(f"Error decoding HTML: {e}, html_bytes: {html_bytes}")

    return extract_plain_text(html_str)
    
    