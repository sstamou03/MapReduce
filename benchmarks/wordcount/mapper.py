"""
Benchmark 1 — Word Count (Mapper)
Classic MapReduce benchmark. Splits text into words and emits (word, 1) pairs.
"""

def map(text_content: str) -> list:
    """
    Input:  raw text string (a chunk of the input file)
    Output: list of [word, count] pairs, e.g. [["hello", 1], ["world", 1], ...]
    """
    results = []
    for line in text_content.splitlines():
        for word in line.strip().split():
            # normalize: lowercase, strip punctuation
            cleaned = word.lower().strip(".,;:!?\"'()-[]{}") 
            if cleaned:
                results.append([cleaned, 1])
    return results
