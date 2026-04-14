"""
Benchmark 2 — Mean Temperature per Year (Reducer)
Aggregates [year, temperature] pairs and computes the average temperature per year.
"""
import json

def reduce(text_content: str) -> dict:
    """
    Input:  JSON string containing a list of [year, temperature] pairs from all mappers
    Output: dict of {year: {"mean": avg, "count": n, "sum": total}}
    """
    pairs = json.loads(text_content)
    
    # Accumulate sum and count per year
    yearly = {}
    for year, temp in pairs:
        year = str(year)
        if year not in yearly:
            yearly[year] = {"sum": 0.0, "count": 0}
        yearly[year]["sum"] += temp
        yearly[year]["count"] += 1

    # Calculate mean per year
    result = {}
    for year in sorted(yearly.keys()):
        data = yearly[year]
        mean = round(data["sum"] / data["count"], 2)
        result[year] = {
            "mean_temperature": mean,
            "measurements": data["count"]
        }
    
    return result
