# SouthDetroit Data Scraper

A Python-based web scraper designed to extract contact information from company websites.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create and edit `.env` file:
```
HUNTER_API_KEY=your_hunter_api_key_here
SERPAPI_KEY=your_serpapi_key_here
```

## Usage

1. Place input CSV in `input/NY_PE_Company_List.csv`
2. Run the pipeline:
```bash
python scripts/south_detroit_pipeline.py
```

## Output

- `output/team_overview.csv`: Main output file
- `output/backups/`: Automatic backups
- `debug_dumps/`: Debug information
- `pipeline.log`: Execution logs
