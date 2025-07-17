import os
import shutil
import re
from pathlib import Path

# Directory with source files
source_dir = Path("./archive")
# Target directory
target_root = Path("./archive_sorted")

# Extract date from file name
pattern = re.compile(r'^(\d{2})\.(\d{2})\.(\d{4})_.*\.json$')

for file in source_dir.glob("*.json"):
    match = pattern.match(file.name)
    if not match:
        print(f"Skipped: {file.name}")
        continue

    day, month, year = match.groups()
    target_dir = target_root / year / month / day
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / file.name
    shutil.move(str(file), str(target_file))
    print(f"Moved: {file.name} -> {target_file}")
