from utils.url import parse_url_source, clear_url_params


def parse_product_article(url: str) -> str:
    source = parse_url_source(url)
    match source:
        case "ozon":
            return parse_ozon_article(url)
        case "detmir":
            return parse_detmir_article(url)
        case "wildberries":
            return parse_wildberries_article(url)
        case "yandex_market":
            return parse_yandex_market_article(url)
        
    return ""

# ozon domain
# /product
# /{name-article}
def parse_ozon_article(url: str) -> str:
    url = clear_url_params(url)
    url = url.strip("https://")

    fields = url.split("/")
    if len(fields) < 3:
        return ""

    if fields[1] != "product":
        return ""

    ind = fields[2].rfind('-')
    article = fields[2][ind+1:]
    if not article.isdigit():
        return ""
    
    return article

# detmir domain
# /product
# /index
# /id
# /{article}
def parse_detmir_article(url: str) -> str:
    url = clear_url_params(url)
    url = url.strip("https://")

    fields = url.split("/")
    if len(fields) < 5:
        return ""

    if fields[1] != "product" or fields[2] != "index" or fields[3] != "id":
        return ""

    article = fields[4]
    if not article.isdigit():
        return ""

    return article

# wildberries domain
# /catalog
# /{article}
# /detail.aspx
def parse_wildberries_article(url: str) -> str:
    url = clear_url_params(url)
    url = url.strip("https://")

    fields = url.split("/")
    if len(fields) < 4:
        return ""

    if fields[1] != "catalog" or fields[3] != "detail.aspx":
        return ""

    article = fields[2]
    if not article.isdigit():
        return ""
    
    return article
    
# yandex_market domain
# /card
# /{name}
# /{article}
def parse_yandex_market_article(url: str) -> str:
    url = clear_url_params(url)
    url = url.strip("https://")

    fields = url.split("/")
    if len(fields) < 4:
        return ""

    if fields[1] != "card" or fields[2] == '':
        return ""

    article = fields[3]
    if not article.isdigit():
        return ""
    
    return article