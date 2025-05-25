import re

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
PHONE_NUMBER_REGEX = r"(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(?:(?:ext|x|ext\.)\s*\d{2,5})?"
IP_ADDRESS_REGEX = r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"

EMAIL_PLACEHOLDER = "|||EMAIL_ADDRESS|||"
PHONE_NUMBER_PLACEHOLDER = "|||PHONE_NUMBER|||"
IP_ADDRESS_PLACEHOLDER = "|||IP_ADDRESS|||"


pii_type_to_regex_and_placeholder_map = {
    "email": (EMAIL_REGEX, EMAIL_PLACEHOLDER),
    "phone_number": (PHONE_NUMBER_REGEX, PHONE_NUMBER_PLACEHOLDER),
    "ip_address": (IP_ADDRESS_REGEX, IP_ADDRESS_PLACEHOLDER),
}

def mask_pii(text: str, pii_type: str):
    regex, placeholder = pii_type_to_regex_and_placeholder_map[pii_type]

    found_matches = re.findall(regex, text, flags=re.IGNORECASE)
    count = len(found_matches)

    modified_text = re.sub(regex, placeholder, text, flags=re.IGNORECASE)

    return (modified_text, count)
