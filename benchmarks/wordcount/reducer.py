"""
Benchmark 1 — Word Count (Reducer)
Aggregates (word, 1) pairs into (word, total_count).
"""
import json

def reduce(text_content: str) -> dict:
    """
    Input:  JSON string containing a list of [word, count] pairs from all mappers
    Output: dict of {word: total_count}
    """
    pairs = json.loads(text_content)
    counts = {}
    for word, count in pairs:
        counts[word] = counts.get(word, 0) + count
    return counts
