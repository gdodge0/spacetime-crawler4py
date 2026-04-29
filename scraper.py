import re
from urllib.parse import urlparse
from src import data, normalization, rules, page_ops
from bs4 import BeautifulSoup

hosts: dict = {}

data.replay_from_jsonl(hosts)


def create_host_ifndef(host_str):
    if host_str not in hosts:
        hosts[host_str] = data.Host(host_str)


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!

    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    # Resolve host/pattern up front so we can register status even when the
    # response is an error, and we'll bail before reaching the simhash step.
    processed_url = normalization.normalize_url(url)
    bucket_keys = processed_url["bucket_keys"]
    host_str = processed_url["normalized_urlsplit"]["netloc"]

    host = None
    if host_str:
        create_host_ifndef(host_str)
        host = hosts[host_str]
        for key in bucket_keys:
            host.create_pattern_ifndef(key)
            host.patterns[key].register_status(resp.status)

    if not (rules.status_ok(resp.status)):
        return list() # If something is wrong, we're going to skip it.

    if resp.raw_response is None or (not rules.headers_ok(resp.raw_response.headers)):
        return list()

    if not rules.size_ok(resp.raw_response.content):
        return list()

    expect_redirect, redirect_url = rules.check_redirect(url, resp)

    # Either return nothing (if some sort of parsing error, or the redirect url)
    if expect_redirect and not redirect_url:
        return list()
    elif expect_redirect and redirect_url:
        return [redirect_url]
    # Else: Continue

    soup = BeautifulSoup(resp.raw_response.content, "lxml")

    base_url = getattr(resp.raw_response, "url", None) or url

    links = page_ops.extract_links(base_url, soup)
    text = page_ops.extract_visible_text(soup)

    # Compute simhash once; reuse for trap detection and for the jsonl
    # log entry so replay on startup is exact (no rehashing).
    sh = data.compute_simhash(text)

    for key in bucket_keys:
        host.patterns[key].register_page(url, text, sh)

    data.write_page(url, text, sh.value, bucket_keys)

    return links


def pattern_allowed(url):
    # For re-check at dequeue time.
    processed_url = normalization.normalize_url(url)
    host_str = processed_url["normalized_urlsplit"]["netloc"]
    if not host_str:
        return True
    host = hosts.get(host_str)
    if host is None:
        return True
    for key in processed_url["bucket_keys"]:
        if not host.pattern_enabled(key):
            return False
    return True


def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    # First, let's normalize this url
    processed_url = normalization.normalize_url(url)
    host_str = processed_url["normalized_urlsplit"]["netloc"]

    # Let's check if this hostname is in our crawler's scope
    if not rules.host_in_scope(host_str):
        return False

    create_host_ifndef(host_str)
    host = hosts[host_str]
    # Let's see if we've seen this path before
    if host.seen_path(processed_url["dedup_key"]):
        return False # don't re-crawl it

    # Now, lets check if this url pattern is enabled
    bucket_keys = processed_url["bucket_keys"]
    for key in bucket_keys:
        if not host.pattern_enabled(key):
            return False # Pattern is disabled due to similarity


    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|java|xml|war|sql|sh|svg|fig|conf|class)$",
            parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
