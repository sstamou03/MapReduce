"""
Benchmark 2 — Mean Temperature per Year (Mapper)
Classic MapReduce example from the original Hadoop paper.
Parses CSV meteorological data and emits [year, temperature] pairs.
"""

def map(text_content: str) -> list:
    """
    Input:  CSV text with lines formatted as: date,region,temperature
            Example: 2005-07-15,Athens,34.2
    Output: list of [year, temperature] pairs
            Example: [["2005", 34.2], ["2005", 31.0], ...]
    """
    results = []
    for line in text_content.splitlines():
        line = line.strip()
        if not line or line.startswith("date"):  # skip empty lines and header
            continue
        try:
            parts = line.split(",")
            date_str = parts[0].strip()
            temperature = float(parts[2].strip())
            year = date_str.split("-")[0]
            results.append([year, temperature])
        except (IndexError, ValueError):
            # Skip malformed lines
            continue
    return results
