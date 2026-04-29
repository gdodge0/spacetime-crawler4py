import json
import sys
from pathlib import Path

from src.normalization import normalize_url


def iter_records(text_dir: Path):
    for jsonl_path in sorted(text_dir.glob("*.jsonl")):
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def compute_stats(text_dir: Path) -> dict:
    unique_pages: set[str] = set()
    longest_url: str | None = None
    longest_word_count = 0
    subdomains: dict[str, set[str]] = {}

    for entry in iter_records(text_dir):
        fetch_url = entry.get("fetch_url")
        if not fetch_url:
            continue
        norm = normalize_url(fetch_url)
        dedup_key = norm["dedup_key"]
        host = norm["normalized_urlsplit"]["netloc"]
        if not dedup_key or not host:
            continue

        if dedup_key in unique_pages:
            pass
        unique_pages.add(dedup_key)
        subdomains.setdefault(host, set()).add(dedup_key)

        text = entry.get("text") or ""
        word_count = sum(1 for _ in text.split())
        if word_count > longest_word_count:
            longest_word_count = word_count
            longest_url = norm["fetch_url"]

    return {
        "unique_pages": len(unique_pages),
        "longest_page_url": longest_url,
        "longest_page_word_count": longest_word_count,
        "subdomains": sorted(
            ((host, len(pages)) for host, pages in subdomains.items()),
            key=lambda item: item[0],
        ),
    }


def print_stats(stats: dict) -> None:
    print(f"Unique pages: {stats['unique_pages']}")
    print()
    print("Longest page (by word count):")
    if stats["longest_page_url"]:
        print(f"  {stats['longest_page_url']}")
        print(f"  {stats['longest_page_word_count']} words")
    else:
        print("  (none)")
    print()
    print(f"Subdomains ({len(stats['subdomains'])}):")
    for host, count in stats["subdomains"]:
        print(f"  {host}, {count}")


def main(argv: list[str]) -> int:
    text_dir = Path(argv[1]) if len(argv) > 1 else Path("text")
    if not text_dir.exists():
        print(f"No text directory at {text_dir}", file=sys.stderr)
        return 1
    print_stats(compute_stats(text_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
