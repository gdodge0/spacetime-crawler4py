import json
import sys
import re
from pathlib import Path
from collections import Counter

from src.normalization import normalize_url


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


def get_words(text: str) -> list[str]:
    return re.findall(r"[a-z]+(?:'[a-z]+)?", text.lower())


def compute_stats(text_dir: Path) -> dict:
    unique_pages: set[str] = set()
    longest_url: str | None = None
    longest_word_count = 0

    top_10_largest_pages = []
    word_counter = Counter()
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
            continue

        unique_pages.add(dedup_key)
        subdomains.setdefault(host, set()).add(dedup_key)

        text = entry.get("text") or ""
        words = get_words(text)
        word_count = len(words)

        if word_count > longest_word_count:
            longest_word_count = word_count
            longest_url = norm["fetch_url"]

        top_10_largest_pages.append((word_count, norm["fetch_url"]))
        top_10_largest_pages.sort(reverse=True)
        top_10_largest_pages = top_10_largest_pages[:10]

        for word in words:
            if len(word) <= 1:
                continue

            if word in STOP_WORDS:
                continue

            word_counter[word] += 1

    return {
        "unique_pages": len(unique_pages),
        "longest_page_url": longest_url,
        "longest_page_word_count": longest_word_count,
        "top_10_largest_pages": top_10_largest_pages,
        "top_50_words": word_counter.most_common(50),
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

    print("Top 10 largest pages:")
    for count, url in stats["top_10_largest_pages"]:
        print(f"  {url}, {count} words")
    print()

    print("Top 50 words:")
    for word, count in stats["top_50_words"]:
        print(f"  {word}, {count}")
    print()

    print(f"Subdomains ({len(stats['subdomains'])}):")
    for host, count in stats["subdomains"]:
        print(f"  {host}, {count}")


def write_stats(stats: dict, output_file: Path = Path("crawler_stats.txt")) -> None:
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Unique pages: {stats['unique_pages']}\n\n")

        f.write("Longest page (by word count):\n")
        if stats["longest_page_url"]:
            f.write(f"  {stats['longest_page_url']}\n")
            f.write(f"  {stats['longest_page_word_count']} words\n")
        else:
            f.write("  (none)\n")
        f.write("\n")

        f.write("Top 10 largest pages:\n")
        for count, url in stats["top_10_largest_pages"]:
            f.write(f"  {url}, {count} words\n")
        f.write("\n")

        f.write("Top 50 words:\n")
        for word, count in stats["top_50_words"]:
            f.write(f"  {word}, {count}\n")
        f.write("\n")

        f.write(f"Subdomains ({len(stats['subdomains'])}):\n")
        for host, count in stats["subdomains"]:
            f.write(f"  {host}, {count}\n")


def main(argv: list[str]) -> int:
    text_dir = Path(argv[1]) if len(argv) > 1 else Path("text")

    if not text_dir.exists():
        print(f"No text directory at {text_dir}", file=sys.stderr)
        return 1

    stats = compute_stats(text_dir)

    print_stats(stats)
    write_stats(stats)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))