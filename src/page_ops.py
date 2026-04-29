from bs4 import BeautifulSoup
from src.normalization import normalize_url
from urllib.parse import urljoin

def extract_links(url, soup):
    links = []

    for link in soup.find_all("a"):
        href = link.get("href")

        if not href:
            continue

        try:
            new_url = urljoin(url, href)
        except ValueError:
            # malformed href
            continue
        res = normalize_url(new_url)

        if not res["dedup_key"]:
            continue # invalid link

        links.append(res["fetch_url"])

    return links


_BOILERPLATE_TAGS = [
    "script", "style", "head", "meta", "title", "noscript",
    "nav", "header", "footer", "aside",
]

_BOILERPLATE_ROLES = {
    "navigation", "banner", "contentinfo", "complementary", "search",
}


def extract_visible_text(soup):
    for tag in soup(_BOILERPLATE_TAGS):
        tag.decompose()

    for tag in soup.find_all(attrs={"role": True}):
        if tag.get("role") in _BOILERPLATE_ROLES:
            tag.decompose()

    visible_text = soup.get_text(separator="\n", strip=True)

    return visible_text