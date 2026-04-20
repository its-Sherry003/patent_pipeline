# Patent Data Pipeline

## Setup
1. Clone the repository
2. Create virtual environment: `python -m venv venv`
3. Activate: `venv\Scripts\activate` (Windows) or `source venv/bin/activate` (Mac/Linux)
4. Install dependencies: `pip install -r requirements.txt`
5. Place the downloaded TSV zip files in the `data/` folder

## Run
python run_full_pipeline.py

## Output
- Database: `output/patent_pipeline.db`
- Clean CSVs: `output/clean_*.csv`
- Reports: `output/top_*.csv`, `output/report.json`
- Console output printed during execution