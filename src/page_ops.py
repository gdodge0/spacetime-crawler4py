from bs4 import BeautifulSoup
from src.normalization import normalize_url
from urllib.parse import urljoin

def extract_links(url, soup):
    base_url = url
    base_tag = soup.find("base", href=True)
    if base_tag:
        base_href = (base_tag.get("href") or "").strip()
        if base_href:
            base_url = urljoin(url, base_href)

    candidates = []
    for tag in soup.find_all(["a", "area"]):
        href = tag.get("href")
        if href:
            candidates.append(href)
    for tag in soup.find_all("iframe"):
        src = tag.get("src")
        if src:
            candidates.append(src)
    for tag in soup.find_all("link"):
        rel = tag.get("rel") or []
        if "canonical" in rel:
            href = tag.get("href")
            if href:
                candidates.append(href)

    seen = set()
    links = []
    for raw in candidates:
        href = raw.strip()
        if not href:
            continue

        try:
            new_url = urljoin(base_url, href)
        except ValueError:
            # malformed href
            continue
        res = normalize_url(new_url)

        if not res["dedup_key"]:
            continue # invalid link

        fetch = res["fetch_url"]
        if fetch in seen:
            continue
        seen.add(fetch)
        links.append(fetch)

    return links


_BOILERPLATE_TAGS = [
    "script", "style", "head", "meta", "title", "noscript",
    "nav", "header", "footer", "aside",
]

_BOILERPLATE_ROLES = {
    "navigation", "banner", "contentinfo", "complementary", "search",
}

_HTML_MARKERS = ("<html", "<body", "<!doctype")


def looks_like_html(content) -> bool:
    if not content:
        return False
    if isinstance(content, bytes):
        sample = content[:8192].decode("utf-8", errors="ignore").lower()
    else:
        sample = content[:8192].lower()
    return any(m in sample for m in _HTML_MARKERS)


def extract_visible_text(soup, content):
    if not looks_like_html(content):
        return ""

    for tag in soup(_BOILERPLATE_TAGS):
        tag.decompose()

    for tag in soup.find_all(attrs={"role": True}):
        if tag.attrs is None:
            continue
        if tag.get("role") in _BOILERPLATE_ROLES:
            tag.decompose()

    visible_text = soup.get_text(separator="\n", strip=True)

    return visible_text