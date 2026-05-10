"""
Generate a ~1 GB temperature CSV dataset.
Format: date,region,temperature
"""
import random
import os

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data.csv")
TARGET_SIZE_BYTES = 200 * 1024 * 1024  # 200 MB

regions = ["Athens", "Thessaloniki", "Heraklion", "Patras", "Larissa",
           "Volos", "Ioannina", "Kavala", "Chania", "Alexandroupoli",
           "Kalamata", "Serres", "Rhodes", "Corfu", "Mytilene"]

# Temperature ranges per month (min, max) for realistic Greek weather
month_temp_ranges = {
    1:  (2.0, 14.0),
    2:  (3.0, 15.0),
    3:  (6.0, 19.0),
    4:  (10.0, 23.0),
    5:  (15.0, 28.0),
    6:  (20.0, 34.0),
    7:  (23.0, 38.0),
    8:  (23.0, 39.0),
    9:  (18.0, 33.0),
    10: (13.0, 26.0),
    11: (8.0, 19.0),
    12: (4.0, 15.0),
}

def generate_dataset():
    written = 0
    line_count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        header = "date,region,temperature\n"
        f.write(header)
        written += len(header)

        year = 1950
        while written < TARGET_SIZE_BYTES:
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            region = random.choice(regions)
            temp_min, temp_max = month_temp_ranges[month]
            temperature = round(random.uniform(temp_min, temp_max), 1)

            line = f"{year}-{month:02d}-{day:02d},{region},{temperature}\n"
            f.write(line)
            written += len(line)
            line_count += 1

            # Cycle through years 1950-2025
            if line_count % 100 == 0:
                year = random.randint(1950, 2025)

            if line_count % 5_000_000 == 0:
                pct = (written / TARGET_SIZE_BYTES) * 100
                print(f"  Progress: {pct:.1f}% ({written / (1024**2):.0f} MB, {line_count:,} lines)")

    final_size = os.path.getsize(OUTPUT_FILE)
    print(f"\nDone! Generated {line_count:,} lines")
    print(f"File size: {final_size / (1024**3):.2f} GB ({final_size / (1024**2):.0f} MB)")

if __name__ == "__main__":
    generate_dataset()
