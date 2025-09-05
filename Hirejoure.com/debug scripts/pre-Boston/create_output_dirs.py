from pathlib import Path

for folder in ["output/csv", "output/debug_dumps", "output/people"]:
    path = Path(folder)
    path.mkdir(parents=True, exist_ok=True)
    print(f"✅ Created: {path.resolve()}")
