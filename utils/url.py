from urllib.parse import urlparse, parse_qs


source_domains = {
    "www.detmir.ru": "detmir",
    "detmir.ru": "detmir",
    "www.ozon.ru": "ozon",
    "ozon.ru": "ozon",
    "www.wildberries.ru": "wildberries",
    "wildberries.ru": "wildberries",
    "www.market.yandex.ru": "yandex_market",
    "market.yandex.ru": "yandex_market"
}

def parse_url_source(url: str) -> str:
    ind = url.find("?")
    if ind != -1:
        url = url[:ind]

    url = url.lstrip("https://").lstrip("http://")
    prefix = url.split("/")[0]

    if prefix in source_domains:
        return source_domains[prefix]
    
    return ""

def parse_url_params(url: str) -> dict:
    p_url = urlparse(url)
    qs = p_url.query
    
    p_qs = parse_qs(qs)
    return p_qs

def clear_url_params(url: str) -> str:
    ind = url.find('?')
    if ind != -1:
        url = url[:ind]
    
    url = url.strip('/') + '/'
    return url

def format_url(url: str) -> str:
    url = url.strip()
    source = parse_url_source(url)
    match source:
        case "ozon":
            return format_ozon_url(url)
        case "detmir":
            return format_detmir_url(url)
        case "wildberries":
            return format_wildberries_url(url)
        case "yandex_market":
            return format_yandex_market_url(url)
        
    return ""

def format_ozon_url(url: str) -> str:
    url = clear_url_params(url)
    return url

def format_detmir_url(url: str) -> str:
    params = parse_url_params(url)
    url = clear_url_params(url)
    if "variant_id" in params:
        url += f"?variant_id={params["variant_id"][0]}"
    return url

def format_wildberries_url(url: str) -> str:
    params = parse_url_params(url)
    url = clear_url_params(url)
    if "size" in params:
        url += f"?size={params["size"][0]}"
    return url

def format_yandex_market_url(url: str) -> str:
    url = clear_url_params(url)
    return url

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