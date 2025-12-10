translation_table = {ord(char): None for char in (chr(i) for i in range(0x110000)) if char.isspace()}
_currency_format = {
    "₽": "RUB",
    "RUR": "RUB"
}

def parse_price(text: str) -> int:
    text = text.translate(translation_table)
    for ind, char in enumerate(text):
        if not char.isdigit():
            num = text[:ind]
            if num == '':
                return 0
            
            return num

    return int(text)

def parse_currency(text: str) -> str:
    text = text.translate(translation_table)
    for i in range(len(text)-1, 0, -1):
        if text[i].isdigit():
            return text[i+1:]

    return text

def format_currency(text: str) -> str:
    if text in _currency_format:
        return _currency_format[text]
    
    return text
