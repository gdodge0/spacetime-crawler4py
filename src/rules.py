from urllib.parse import urljoin
from src.normalization import normalize_url

def host_in_scope(host: str):
    if not host:
        return False

    hostname_split = host.split(".")

    if len(hostname_split) < 3:
        return False
    elif not ((hostname_split[-1] == "edu") and (hostname_split[-2] == "uci") and (
            hostname_split[-3] in ["ics", "cs", "informatics", "stat"])):
        return False

    return True

def check_redirect(url, resp):
    if resp.status not in range(301, 309):
        return False, None

    new_location = resp.headers.get("location")
    if not new_location:
        return True, None

    redirect_url = urljoin(url, new_location)
    redirect_url = normalize_url(redirect_url)["fetch_url"]

    if not redirect_url:
        return True, None

    return True, redirect_url


def status_ok(status):
    if status == 608:
        return False # robots.txt
    elif status == 601:
        return False  # download exception
    elif status in range(400, 600):
        return False  # error page of some nature

    return True


MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


def headers_ok(headers):
    content_type = headers.get("content-type")
    if (content_type is None) or (not "text/html" in content_type.lower()):
        return False

    content_length = headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > MAX_FILE_SIZE:
                return False
        except ValueError:
            pass

    return True


def size_ok(content):
    if content is None:
        return True
    return len(content) <= MAX_FILE_SIZE