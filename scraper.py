import re
from collections import Counter, defaultdict

from urllib.parse import urlparse, urljoin, urldefrag, urlunparse, parse_qsl

from bs4 import BeautifulSoup


# more efficient
BAD_EXTENSIONS = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|"
    r"mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|"
    r"names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|"
    r"sha1|thmx|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)$"
)

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't",
    "cannot", "could", "couldn't", "did", "didn't", "do", "does",
    "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have",
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
    "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is",
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more",
    "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
    "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd",
    "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them",
    "themselves", "then", "there", "there's", "these", "they", "they'd",
    "they'll", "they're", "they've", "this", "those", "through", "to",
    "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
    "we'll", "we're", "we've", "were", "weren't", "what", "what's",
    "when", "when's", "where", "where's", "which", "while", "who",
    "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
    "yours", "yourself", "yourselves"
}

DOMAINS = {
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
}

unique_pages = set()
word_counter = Counter()
subdomain_counter = defaultdict(int)

longest_page_url = ""
longest_page_word_count = 0


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

    if resp is None or resp.status is None or resp.status != 200:  
        return []
    
    if resp.raw_response is None:
        return []
    
    if not hasattr(resp.raw_response, "content") or resp.raw_response.content is None:
        return []
    
    content = resp.raw_response.content

    if len(content) ==0:
        return []
    
    # if content type exist and says it is not html skip it
    headers = getattr(resp.raw_response, "headers", {})
    content_type = headers.get("content-type", "").lower()
    
    if content_type and "html" not in content_type:
        return []
    
    base_url = getattr(resp.raw_response, "url", None) or resp.url or url
    base_url, _ = urldefrag(base_url)


    if not is_valid(base_url):
        return[]
    
    soup = BeautifulSoup(content,"html.parser")

    text = soup.get_text(separator=" ")
    update_stats(base_url, text)


    links = set()

    for tag in soup.find_all("a", href = True):
        link = urljoin(base_url, tag["href"])
        link, _ = urldefrag(link)

        if is_valid(link):
            links.add(link)


    return list(links)

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
            
        if parsed.hostname:
            hostname = parsed.hostname.lower()
        else:
            hostname = ""


        allowed = False

        for domain in DOMAINS:
            if hostname == domain or hostname.endswith("." + domain):
                allowed = True
                break

        if not allowed:
            return False
            
        path = parsed.path.lower()

        if BAD_EXTENSIONS.match(path):
            return False
        
        if is_trap(parsed):
            return False


        return True
    



    except Exception:
        return False



def is_trap(parsed):
    url = parsed.geturl()
    path = parsed.path.lower()

    if len(url) > 300:
        return True
    
    segments = []

    for seg in path.split("/"):
        if seg:
            segments.append(seg)

    
    if len(segments) > 15:
        return True
    
    counts = Counter(segments)


    # repeated path segments
    for count in counts.values():
        if count >= 3:
            return True
        
    return False


def update_stats(url,text):

    #will finish later
    pass