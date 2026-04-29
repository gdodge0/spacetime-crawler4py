import json
import logging
import re
from simhash import Simhash, SimhashIndex
from pathlib import Path
from src.normalization import normalize_url
from time import time

_log = logging.getLogger(__name__)

# Trap detection: bucket fetched pages by URL-path pattern, detect similar pages w/ simhash,
# skip when threshold is reached, and write log to jsonl.

MIN_PAGES_SKIP = 20
MAX_PAGES_COMPARE = 10000
MAX_URLS_PER_PATTERN = 10000  # hard cap on distinct fetched URLs per bucket
NEAR_DUP_TRIGGER = 20
MIN_WORDS = 50
LOW_VALUE_TRIGGER = 20
MIN_REQUESTS_FOR_ERROR_CHECK = 20
ERROR_RATE_THRESHOLD = 0.80

TEXT_DIR = "text"


def get_features(text: str) -> list[str]:
    return re.findall(r"\w+", text.lower())


def compute_simhash(text: str) -> Simhash:
    return Simhash(get_features(text))


class Pattern:
    pattern: str
    pages: SimhashIndex
    pages_count: int
    urls_seen: int
    low_value_count: int
    requests_count: int
    error_count: int
    pattern_enabled: bool

    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self.pages = SimhashIndex([])
        self.pages_count = 0
        self.urls_seen = 0
        self.low_value_count = 0
        self.requests_count = 0
        self.error_count = 0
        self.pattern_enabled = True

    def _disable(self, reason: str) -> None:
        # Only log on the first transition; the disable checks re-run on
        # later registrations and we don't want duplicate warnings.
        if self.pattern_enabled:
            _log.warning("Pattern banned (%s): %s", reason, self.pattern)
        self.pattern_enabled = False

    def register_simhash(self, url: str, sh: Simhash) -> None:
        self.urls_seen += 1
        if self.urls_seen > MAX_URLS_PER_PATTERN:
            self._disable(f"urls_seen>{MAX_URLS_PER_PATTERN}")
            return

        if self.pages_count > MIN_PAGES_SKIP:
            near_dup_count = len(self.pages.get_near_dups(sh))
            if near_dup_count > NEAR_DUP_TRIGGER:
                self._disable(f"near_dups={near_dup_count}>{NEAR_DUP_TRIGGER}")

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
            error_rate = self.error_count / self.requests_count
            if error_rate > ERROR_RATE_THRESHOLD:
                self._disable(f"error_rate={error_rate:.2f}>{ERROR_RATE_THRESHOLD}")

    def register_low_value(self) -> None:
        self.low_value_count += 1
        if self.low_value_count > LOW_VALUE_TRIGGER:
            self._disable(f"low_value={self.low_value_count}>{LOW_VALUE_TRIGGER}")

    def register_page(self, url: str, text: str, sh: Simhash) -> None:
        self.register_simhash(url, sh)
        if len(get_features(text)) < MIN_WORDS:
            self.register_low_value()

    def register_text(self, url: str, text: str) -> None:
        self.register_page(url, text, compute_simhash(text))


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

    text = entry.get("text", "")
    sh_value = entry.get("simhash")
    if isinstance(sh_value, int):
        sh = Simhash(value=sh_value)
    elif text:
        sh = compute_simhash(text)
    else:
        return True  # path recorded as seen, but no body to hash

    bucket_keys = entry.get("bucket_keys") or norm["bucket_keys"]
    for key in bucket_keys:
        host.create_pattern_ifndef(key)
        host.patterns[key].register_page(fetch_url, text, sh)

    return True
