import json
import re
from simhash import Simhash, SimhashIndex
from pathlib import Path
from src.normalization import normalize_url
from time import time

# Trap detection: bucket fetched pages by URL-path pattern, detect similar pages w/ simhash,
# skip when threshold is reached, and write log to jsonl.

MIN_PAGES_SKIP = 10
MAX_PAGES_COMPARE = 1000
MAX_URLS_PER_PATTERN = 1000  # hard cap on distinct fetched URLs per bucket
NEAR_DUP_RATIO = 0.7
MIN_REQUESTS_FOR_ERROR_CHECK = 10
ERROR_RATE_THRESHOLD = 0.7

TEXT_DIR = "text"


def get_features(s: str) -> list[str]:
    # https://leons.im/posts/a-python-implementation-of-simhash-algorithm/
    width = 6
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


def compute_simhash(text: str) -> Simhash:
    return Simhash(get_features(text))


class Pattern:
    pattern: str
    pages: SimhashIndex
    pages_count: int
    urls_seen: int
    requests_count: int
    error_count: int
    pattern_enabled: bool

    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self.pages = SimhashIndex([])
        self.pages_count = 0
        self.urls_seen = 0
        self.requests_count = 0
        self.error_count = 0
        self.pattern_enabled = True

    def register_simhash(self, url: str, sh: Simhash) -> None:
        self.urls_seen += 1
        if self.urls_seen > MAX_URLS_PER_PATTERN:
            self.pattern_enabled = False
            return

        if self.pages_count > MIN_PAGES_SKIP:
            near_dup_count = len(self.pages.get_near_dups(sh))
            ratio = near_dup_count / self.pages_count
            if ratio > NEAR_DUP_RATIO:
                self.pattern_enabled = False

        if self.pattern_enabled and self.pages_count < MAX_PAGES_COMPARE:
            self.pages.add(url, sh)
            self.pages_count += 1

    def register_status(self, status: int) -> None:
        if not 100 <= status < 600:
            return
        self.requests_count += 1
        if 400 <= status < 500:
            self.error_count += 1
        if self.requests_count > MIN_REQUESTS_FOR_ERROR_CHECK:
            if self.error_count / self.requests_count > ERROR_RATE_THRESHOLD:
                self.pattern_enabled = False

    def register_text(self, url: str, text: str) -> None:
        self.register_simhash(url, compute_simhash(text))


class Host:
    host: str
    patterns: dict[str, "Pattern"]
    paths: set[str]

    def __init__(self, host: str) -> None:
        self.host = host
        self.patterns = dict()
        self.paths = set()

    def seen_path(self, path):
        if path in self.paths:
            return True
        self.paths.add(path)
        return False

    def create_pattern_ifndef(self, pattern_str: str):
        if pattern_str not in self.patterns:
            self.patterns[pattern_str] = Pattern(pattern_str)

    def pattern_enabled(self, pattern_str: str) -> bool:
        pattern = self.patterns.get(pattern_str)
        if pattern is None:
            return True
        return pattern.pattern_enabled


def write_page(url: str, text: str, simhash_value: int, bucket_keys: list[str]) -> None:
    norm = normalize_url(url)
    host = norm["normalized_urlsplit"]["netloc"]
    if not host:
        return

    file_path = Path(TEXT_DIR) / f"{host}.jsonl"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "fetch_url": norm["fetch_url"],
        "dedup_key": norm["dedup_key"],
        "bucket_keys": bucket_keys,
        "simhash": simhash_value,
        "fetched_at": time(),
        "text": text,
    }
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record))
        f.write("\n")


def replay_from_jsonl(hosts: dict, text_dir: str = TEXT_DIR) -> int:
    """Rebuild in-memory host/pattern state from the jsonl log.

    Returns the number of records replayed.
    """
    p = Path(text_dir)
    if not p.exists():
        return 0

    replayed = 0
    for jsonl_path in sorted(p.glob("*.jsonl")):
        try:
            f = open(jsonl_path, "r", encoding="utf-8")
        except OSError:
            continue
        with f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _replay_entry(hosts, entry):
                    replayed += 1
    return replayed


def _replay_entry(hosts: dict, entry: dict) -> bool:
    fetch_url = entry.get("fetch_url")
    if not fetch_url:
        return False

    norm = normalize_url(fetch_url)
    dedup_key = norm["dedup_key"]
    host_str = norm["normalized_urlsplit"]["netloc"]
    if not dedup_key or not host_str:
        return False

    host = hosts.get(host_str)
    if host is None:
        host = Host(host_str)
        hosts[host_str] = host

    host.paths.add(dedup_key)

    sh_value = entry.get("simhash")
    if isinstance(sh_value, int):
        sh = Simhash(value=sh_value)
    else:
        text = entry.get("text", "")
        if not text:
            return True  # path recorded as seen, but no body to hash
        sh = compute_simhash(text)

    bucket_keys = entry.get("bucket_keys") or norm["bucket_keys"]
    for key in bucket_keys:
        host.create_pattern_ifndef(key)
        host.patterns[key].register_simhash(fetch_url, sh)

    return True
