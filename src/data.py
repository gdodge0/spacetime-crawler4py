import json
import logging
import re
import hashlib
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
SUBTREE_BAN_TRIGGER = 3  # ban an ancestor once N descendant patterns have banned
SUBTREE_BAN_MIN_DEPTH = 2  # don't cascade above 2 path segments deep

TEXT_DIR = "text"


def get_features(text: str) -> list[str]:
    return re.findall(r"\w+", text.lower())


def compute_simhash(text: str) -> 'Simhash':
    return Simhash(get_features(text))

"--------------------------SIMHASH CODE START--------------------------
class Simhash:
    """
    Simhash implementation from scratch.
    Creates a compact fingerprint of text content for similarity detection.
    """
    def __init__(self, features: list[str], hashbits: int = 64):
        """Compute simhash fingerprint from text features (words)."""
        self.hashbits = hashbits  # Keep the desired fingerprint width for this object
        votes = [0] * hashbits  # One score per bit position, starting at zero

        for feature in features:
            bits = self._feature_bits(feature)  # Convert a word into a fixed-length bit string
            for i, bit in enumerate(bits):
                if bit == '1':
                    votes[i] += 1  # Word votes that bit position i should become 1
                else:
                    votes[i] -= 1  # Word votes that bit position i should become 0

        self.value = self._votes_to_fingerprint(votes)  # Turn the vote totals into an integer fingerprint

    def _feature_bits(self, feature: str) -> str:
        """
        Hash a feature word and return a fixed-length binary string.

        MD5 produces a 128-bit value, but we intentionally keep only hashbits bits.
        """
        hash_obj = hashlib.md5(feature.encode('utf-8'))  # Hash the word bytes
        hash_int = int(hash_obj.hexdigest(), 16)  # Convert the hex digest to an integer
        mask = (1 << self.hashbits) - 1  # Mask to keep exactly hashbits bits
        truncated = hash_int & mask  # Keep the lowest hashbits bits only
        return bin(truncated)[2:].zfill(self.hashbits)  # Return as zero-padded binary string

    def _votes_to_fingerprint(self, votes: list[int]) -> int:
        """
        Convert accumulated +1/-1 vote totals into a final bit fingerprint.

        If vote >= 0, the final bit is 1; otherwise it is 0.
        """
        fingerprint = 0  # Start with no bits set
        for i, vote in enumerate(votes):
            if vote >= 0:
                fingerprint |= 1 << (self.hashbits - 1 - i)  # Set the corresponding bit in the fingerprint
        return fingerprint

    def hamming_distance(self, other: 'Simhash') -> int:
        """
        Count how many bits differ between two simhash fingerprints.
        """
        xor_result = self.value ^ other.value  # XOR shows differing bits as 1s
        return bin(xor_result).count('1')  # Count the number of differing bits


class SimhashIndex:
    """
    Index for storing simhashes and finding near-duplicates.
    Used to detect when too many similar pages have been crawled (trap detection).
    """
    def __init__(self, objs=None):
        """Initialize the index with optional existing (url, simhash) tuples."""
        self.objs = {}  # Map simhash integer -> list of (url, simhash) tuples

        if objs:
            for url, sh in objs:
                self.add(url, sh)  # Add any provided entries to the index

    def add(self, url: str, sh: Simhash):
        """Store a URL and its simhash in the index."""
        if sh.value not in self.objs:
            self.objs[sh.value] = []  # Create a new bucket for new fingerprints
        self.objs[sh.value].append((url, sh))  # Append this page to the bucket for its fingerprint

    def get_near_dups(self, sh: Simhash) -> list[str]:
        """Return URLs whose stored simhash is close enough to the input simhash."""
        near_dups = []  # Collect similar URLs here
        threshold = 3  # Maximum allowed Hamming distance for near-duplicate

        for stored_value, url_simhash_pairs in self.objs.items():
            stored_simhash = url_simhash_pairs[0][1]  # Take the simhash from this bucket
            distance = sh.hamming_distance(stored_simhash)  # Compare similarity

            if distance <= threshold:
                for url, _ in url_simhash_pairs:
                    near_dups.append(url)  # Add every URL that shares this stored fingerprint

        return near_dups
"--------------------------SIMHASH CODE END--------------------------

class Pattern:
    pattern: str
    pages: SimhashIndex
    pages_count: int
    urls_seen: int
    low_value_count: int
    requests_count: int
    error_count: int
    pattern_enabled: bool

    def __init__(self, pattern: str, host: "Host" = None) -> None:
        self.pattern = pattern
        self.host = host
        self.pages = SimhashIndex([])
        self.pages_count = 0
        self.urls_seen = 0
        self.low_value_count = 0
        self.requests_count = 0
        self.error_count = 0
        self.pattern_enabled = True

    def _disable(self, reason: str) -> None:
        # Only fire the cascade and log on the first transition.
        if not self.pattern_enabled:
            return
        self.pattern_enabled = False
        _log.warning("Pattern banned (%s): %s", reason, self.pattern)
        if self.host is not None:
            self.host.on_pattern_disabled(self.pattern)

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
    subtree_ban_counts: dict[str, int]
    banned_subtrees: set[str]

    def __init__(self, host: str) -> None:
        self.host = host
        self.patterns = dict()
        self.paths = set()
        # For cascading ban of related subtrees
        self.subtree_ban_counts = dict()
        self.banned_subtrees = set()

    def seen_path(self, path):
        if path in self.paths:
            return True
        self.paths.add(path)
        return False

    def create_pattern_ifndef(self, pattern_str: str):
        if pattern_str not in self.patterns:
            self.patterns[pattern_str] = Pattern(pattern_str, self)

    def on_pattern_disabled(self, key: str) -> None:
        parts = key.split("/")
        for i in range(len(parts) - 1, 0, -1):
            ancestor = "/".join(parts[:i])
            if not ancestor or ancestor.count("/") < SUBTREE_BAN_MIN_DEPTH:
                continue
            if ancestor in self.banned_subtrees:
                continue
            cnt = self.subtree_ban_counts.get(ancestor, 0) + 1
            self.subtree_ban_counts[ancestor] = cnt
            if cnt >= SUBTREE_BAN_TRIGGER:
                self.banned_subtrees.add(ancestor)
                _log.warning(
                    "Subtree banned (descendants=%d>=%d): %s",
                    cnt, SUBTREE_BAN_TRIGGER, ancestor)

    def pattern_enabled(self, pattern_str: str) -> bool:
        for prefix in self.banned_subtrees:
            if pattern_str == prefix or pattern_str.startswith(prefix + "/"):
                return False
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
