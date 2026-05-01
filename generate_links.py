import json
import re
import sys
from glob import glob


def iter_records(text_dir):
    for jsonl_path in sorted(glob(text_dir + "/*.jsonl")):
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass


def sanitize_filename(word):
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", word.lower()).strip("_")
    return cleaned or "word"


def build_links_for_word(word, text_dir):
    target_word = word.lower()
    seen_pages = set()
    urls = set()

    for entry in iter_records(text_dir):
        dedup_key = entry.get("dedup_key")
        normalized_url = entry.get("fetch_url")

        if not dedup_key or not normalized_url:
            continue

        if dedup_key in seen_pages:
            continue
        seen_pages.add(dedup_key)

        text = (entry.get("text") or "").lower()
        words_on_page = set(re.findall(r"[a-z]+(?:'[a-z]+)?", text))

        if target_word in words_on_page:
            urls.add(normalized_url)

    return sorted(urls)


def write_links(word, urls, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Word: {word}\n")
        f.write(f"Count: {len(urls)}\n")
        f.write("URLs:\n")
        for url in urls:
            f.write(f"{url}\n")


def main():
    argv = sys.argv

    if len(argv) < 2:
        print("Usage: python generate_links.py <word> [text_dir] [output_file]")
        return 1

    word = argv[1].lower()
    text_dir = argv[2] if len(argv) > 2 else "text"
    output_file = argv[3] if len(argv) > 3 else sanitize_filename(word) + "_links.txt"

    if not glob(text_dir + "/*.jsonl"):
        print(f"No jsonl files found in {text_dir}")
        return 1

    urls = build_links_for_word(word, text_dir)
    write_links(word, urls, output_file)
    print(f"Wrote {len(urls)} links for '{word}' to {output_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
