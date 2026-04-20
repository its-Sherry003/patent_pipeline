"""
Patent Data Pipeline – Resumable with Checkpoints
"""

import pandas as pd
import sqlite3
import zipfile
import json
import os
from pathlib import Path
from tqdm import tqdm

# ============================================================
# CONFIGURATION
# ============================================================
DATA_DIR = Path("C:\\dev\\patent_pipeline\\data")
DB_PATH = Path("C:\\dev\\patent_pipeline\\output\\patent_pipeline.db")
CHECKPOINT_FILE = Path("C:\\dev\\patent_pipeline\\output\\pipeline_checkpoint.json")
CHUNK_SIZE = 50000

# Ensure output directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================
# CHECKPOINT MANAGEMENT
# ============================================================
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"completed_tables": []}

def save_checkpoint(completed_tables):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"completed_tables": completed_tables}, f)

def is_table_completed(table_name, checkpoint):
    return table_name in checkpoint["completed_tables"]

def mark_table_completed(table_name, checkpoint):
    if table_name not in checkpoint["completed_tables"]:
        checkpoint["completed_tables"].append(table_name)
        save_checkpoint(checkpoint["completed_tables"])

# ============================================================
# HELPER: Count rows in zipped TSV (for progress bar)
# ============================================================
def count_tsv_rows(zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            tsv_name = z.namelist()[0]
            with z.open(tsv_name) as f:
                return sum(1 for _ in f) - 1  # subtract header
    except:
        return None

def read_tsv_in_chunks(zip_path, columns=None, chunksize=CHUNK_SIZE):
    with zipfile.ZipFile(zip_path, 'r') as z:
        tsv_name = z.namelist()[0]
        with z.open(tsv_name) as f:
            chunk_iter = pd.read_csv(f, sep='\t', usecols=columns, chunksize=chunksize, low_memory=False)
            for chunk in chunk_iter:
                yield chunk

# ============================================================
# TABLE BUILDERS (each checks checkpoint before starting)
# ============================================================
def build_patents(conn, checkpoint):
    if is_table_completed("patents", checkpoint):
        print("  Patents table already completed. Skipping.")
        return
    print("\n Building patents table...")
    patent_abstracts = {}

    abstract_path = DATA_DIR / "g_patent_abstract.tsv.zip"
    if abstract_path.exists():
        print("  Loading abstracts...")
        total_abstracts = count_tsv_rows(abstract_path)
        with tqdm(total=total_abstracts, desc="  Reading abstracts", unit="rows") as pbar:
            for chunk in read_tsv_in_chunks(abstract_path, columns=['patent_id', 'patent_abstract']):
                for _, row in chunk.iterrows():
                    patent_abstracts[row['patent_id']] = row['patent_abstract']
                pbar.update(len(chunk))
    else:
        print("  Warning: g_patent_abstract.tsv.zip not found; abstracts will be empty.")

    patent_path = DATA_DIR / "g_patent.tsv.zip"
    if not patent_path.exists():
        raise FileNotFoundError("g_patent.tsv.zip not found")
    total_patents = count_tsv_rows(patent_path)
    with tqdm(total=total_patents, desc="  Processing patents", unit="rows") as pbar:
        for chunk in read_tsv_in_chunks(patent_path, columns=['patent_id', 'patent_title', 'patent_date']):
            # Rename columns to match schema
            chunk = chunk.rename(columns={'patent_title': 'title', 'patent_date': 'filing_date'})
            chunk['year'] = pd.to_datetime(chunk['filing_date'], errors='coerce').dt.year
            chunk['abstract'] = chunk['patent_id'].map(patent_abstracts).fillna("No abstract")
            # Insert only the required columns
            chunk[['patent_id', 'title', 'abstract', 'filing_date', 'year']].to_sql('patents', conn, if_exists='append', index=False)
            pbar.update(len(chunk))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_patents_year ON patents(year);")
    mark_table_completed("patents", checkpoint)
    print("   Patents table done.\n")

def build_inventors(conn, checkpoint):
    if is_table_completed("inventors", checkpoint):
        print("  Inventors table already completed. Skipping.")
        return
    print("...Building inventors table...")
    loc_map = {}
    loc_path = DATA_DIR / "g_location_disambiguated.tsv.zip"
    if loc_path.exists():
        print("  Loading location map...")
        total_locs = count_tsv_rows(loc_path)
        with tqdm(total=total_locs, desc="  Reading locations", unit="rows") as pbar:
            for chunk in read_tsv_in_chunks(loc_path, columns=['location_id', 'disambig_country']):
                for _, row in chunk.iterrows():
                    loc_map[row['location_id']] = row['disambig_country']
                pbar.update(len(chunk))
    else:
        print("  Warning: location file missing; countries will be Unknown")
    inv_path = DATA_DIR / "g_inventor_disambiguated.tsv.zip"
    if not inv_path.exists():
        raise FileNotFoundError("g_inventor_disambiguated.tsv.zip not found")
    total_inventors = count_tsv_rows(inv_path)
    with tqdm(total=total_inventors, desc="  Processing inventors", unit="rows") as pbar:
        for chunk in read_tsv_in_chunks(inv_path, columns=['inventor_id', 'inventor_first_name', 'inventor_last_name', 'location_id']):
            chunk['name'] = chunk['inventor_first_name'].fillna('') + ' ' + chunk['inventor_last_name'].fillna('')
            chunk['name'] = chunk['name'].str.strip()
            chunk.loc[chunk['name'] == '', 'name'] = 'Unknown'
            chunk['country'] = chunk['location_id'].map(loc_map).fillna('Unknown')
            df_inv = chunk[['inventor_id', 'name', 'country']].drop_duplicates('inventor_id')
            df_inv.to_sql('inventors', conn, if_exists='append', index=False)
            pbar.update(len(chunk))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inventors_id ON inventors(inventor_id);")
    mark_table_completed("inventors", checkpoint)
    print("   Inventors table done.\n")

def build_companies(conn, checkpoint):
    if is_table_completed("companies", checkpoint):
        print("  Companies table already completed. Skipping.")
        return
    print("...Building companies table...")
    loc_map = {}
    loc_path = DATA_DIR / "g_location_disambiguated.tsv.zip"
    if loc_path.exists():
        total_locs = count_tsv_rows(loc_path)
        with tqdm(total=total_locs, desc="  Reading locations (companies)", unit="rows") as pbar:
            for chunk in read_tsv_in_chunks(loc_path, columns=['location_id', 'disambig_country']):
                for _, row in chunk.iterrows():
                    loc_map[row['location_id']] = row['disambig_country']
                pbar.update(len(chunk))
    assignee_path = DATA_DIR / "g_persistent_assignee.tsv.zip"
    if not assignee_path.exists():
        assignee_path = DATA_DIR / "g_assignee_disambiguated.tsv.zip"
        if not assignee_path.exists():
            raise FileNotFoundError("No assignee file found")
    total_assignees = count_tsv_rows(assignee_path)
    with tqdm(total=total_assignees, desc="  Processing companies", unit="rows") as pbar:
        for chunk in read_tsv_in_chunks(assignee_path, columns=['assignee_id', 'assignee_organization', 'location_id']):
            chunk = chunk.rename(columns={'assignee_organization': 'name'})
            chunk['country'] = chunk['location_id'].map(loc_map).fillna('Unknown')
            df_comp = chunk[['assignee_id', 'name']].drop_duplicates('assignee_id')
            df_comp = df_comp.rename(columns={'assignee_id': 'company_id'})
            df_comp.to_sql('companies', conn, if_exists='append', index=False)
            pbar.update(len(chunk))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_id ON companies(company_id);")
    mark_table_completed("companies", checkpoint)
    print("   Companies table done.\n")

def build_patent_inventor(conn, checkpoint):
    if is_table_completed("patent_inventor", checkpoint):
        print("  Patent-inventor relationship already completed. Skipping.")
        return
    print(" Building patent-inventor relationships...")
    rel_path = DATA_DIR / "g_inventor_not_disambiguated.tsv.zip"
    if not rel_path.exists():
        raise FileNotFoundError("g_inventor_not_disambiguated.tsv.zip not found")
    total_links = count_tsv_rows(rel_path)
    with tqdm(total=total_links, desc="  Processing inventor links", unit="rows") as pbar:
        for chunk in read_tsv_in_chunks(rel_path, columns=['patent_id', 'inventor_id']):
            chunk = chunk.drop_duplicates()
            chunk.to_sql('patent_inventor', conn, if_exists='append', index=False)
            pbar.update(len(chunk))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_patent ON patent_inventor(patent_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_inventor ON patent_inventor(inventor_id);")
    mark_table_completed("patent_inventor", checkpoint)
    print("   Patent-inventor table done.\n")

def build_patent_company(conn, checkpoint):
    if is_table_completed("patent_company", checkpoint):
        print("  Patent-company relationship already completed. Skipping.")
        return
    print(" Building patent-company relationships...")
    rel_path = DATA_DIR / "g_assignee_not_disambiguated.tsv.zip"
    if not rel_path.exists():
        raise FileNotFoundError("g_assignee_not_disambiguated.tsv.zip not found")
    total_links = count_tsv_rows(rel_path)
    with tqdm(total=total_links, desc="  Processing company links", unit="rows") as pbar:
        for chunk in read_tsv_in_chunks(rel_path, columns=['patent_id', 'assignee_id']):
            chunk = chunk.drop_duplicates()
            chunk = chunk.rename(columns={'assignee_id': 'company_id'})
            chunk.to_sql('patent_company', conn, if_exists='append', index=False)
            pbar.update(len(chunk))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_patent ON patent_company(patent_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_company ON patent_company(company_id);")
    mark_table_completed("patent_company", checkpoint)
    print("   Patent-company table done.\n")

# ============================================================
# QUERIES AND REPORTS (same as before)
# ============================================================
def run_queries(conn):
    print("\n" + "="*60)
    print(" RUNNING REQUIRED SQL QUERIES")
    print("="*60)
    queries = {
        "Q1_Top_Inventors": """SELECT i.name, COUNT(pi.patent_id) as patent_count FROM inventors i JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id GROUP BY i.inventor_id ORDER BY patent_count DESC LIMIT 10""",
        "Q2_Top_Companies": """SELECT c.name, COUNT(pc.patent_id) as patent_count FROM companies c JOIN patent_company pc ON c.company_id = pc.company_id GROUP BY c.company_id ORDER BY patent_count DESC LIMIT 10""",
        "Q3_Top_Countries": """SELECT i.country, COUNT(DISTINCT pi.patent_id) as patent_count FROM inventors i JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id GROUP BY i.country ORDER BY patent_count DESC LIMIT 10""",
        "Q4_Trends_Over_Time": """SELECT year, COUNT(*) as patent_count FROM patents WHERE year IS NOT NULL GROUP BY year ORDER BY year""",
        "Q5_Join_Query": """SELECT p.patent_id, p.title, i.name as inventor_name, c.name as company_name FROM patents p LEFT JOIN patent_inventor pi ON p.patent_id = pi.patent_id LEFT JOIN inventors i ON pi.inventor_id = i.inventor_id LEFT JOIN patent_company pc ON p.patent_id = pc.patent_id LEFT JOIN companies c ON pc.company_id = c.company_id LIMIT 20""",
        "Q6_CTE_Query": """WITH yearly_inventor_counts AS (SELECT i.inventor_id, i.name, strftime('%Y', p.filing_date) as year, COUNT(*) as cnt FROM inventors i JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id JOIN patents p ON pi.patent_id = p.patent_id GROUP BY i.inventor_id, year) SELECT name, year, cnt FROM yearly_inventor_counts WHERE cnt > 1 ORDER BY year DESC, cnt DESC LIMIT 20""",
        "Q7_Ranking_Query": """SELECT i.name, COUNT(pi.patent_id) as patent_count, RANK() OVER (ORDER BY COUNT(pi.patent_id) DESC) as rank FROM inventors i JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id GROUP BY i.inventor_id ORDER BY rank LIMIT 20"""
    }
    results = {}
    for name, sql in queries.items():
        print(f"\n--- {name} ---")
        df = pd.read_sql_query(sql, conn)
        print(df.to_string(index=False))
        results[name] = df
    return results

def generate_reports(conn, results):
    print("\n" + "="*60)
    print(" EXPORTING REPORTS")
    print("="*60)
    results["Q1_Top_Inventors"].to_csv("output/top_inventors.csv", index=False)
    results["Q2_Top_Companies"].to_csv("output/top_companies.csv", index=False)
    results["Q3_Top_Countries"].to_csv("output/country_trends.csv", index=False)
    print(" Exported: top_inventors.csv, top_companies.csv, country_trends.csv")
    clean_patents = pd.read_sql_query("SELECT * FROM patents", conn)
    clean_inventors = pd.read_sql_query("SELECT * FROM inventors", conn)
    clean_companies = pd.read_sql_query("SELECT * FROM companies", conn)
    clean_patents.to_csv("output/clean_patents.csv", index=False)
    clean_inventors.to_csv("output/clean_inventors.csv", index=False)
    clean_companies.to_csv("output/clean_companies.csv", index=False)
    print(" Exported clean CSVs: clean_patents.csv, clean_inventors.csv, clean_companies.csv")
    total_patents = pd.read_sql_query("SELECT COUNT(*) as cnt FROM patents", conn).iloc[0]["cnt"]
    top_inventors = results["Q1_Top_Inventors"].head(5).to_dict(orient="records")
    top_companies = results["Q2_Top_Companies"].head(5).to_dict(orient="records")
    top_countries = results["Q3_Top_Countries"].head(5).to_dict(orient="records")
    json_report = {
        "total_patents": total_patents,
        "top_inventors": [{"name": r["name"], "patents": int(r["patent_count"])} for r in top_inventors],
        "top_companies": [{"name": r["name"], "patents": int(r["patent_count"])} for r in top_companies],
        "top_countries": [{"country": r["country"], "patent_count": int(r["patent_count"])} for r in top_countries]
    }
    with open("output/report.json", "w") as f:
        json.dump(json_report, f, indent=2)
    print(" Exported: report.json")

# ============================================================
# MAIN
# ============================================================
def main():
    print(" Starting Patent Data Pipeline (Resumable)")
    print("="*60)

    # Remove checkpoint if we want a fresh start? We'll keep it.
    checkpoint = load_checkpoint()
    print(f" Previous completed tables: {checkpoint['completed_tables']}")

    # Connect to database (keep existing data if any)
    conn = sqlite3.connect(DB_PATH)

    # Ensure schema exists (if fresh db, create tables)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='patents'")
    if not cursor.fetchone():
        schema_path = Path("sql/schema.sql")
        if schema_path.exists():
            with open(schema_path, "r") as f:
                conn.executescript(f.read())
            print(" Created tables from sql/schema.sql")
        else:
            conn.executescript("""
                CREATE TABLE patents (patent_id TEXT PRIMARY KEY, title TEXT, abstract TEXT, filing_date TEXT, year INTEGER);
                CREATE TABLE inventors (inventor_id TEXT PRIMARY KEY, name TEXT, country TEXT);
                CREATE TABLE companies (company_id TEXT PRIMARY KEY, name TEXT);
                CREATE TABLE patent_inventor (patent_id TEXT, inventor_id TEXT, PRIMARY KEY (patent_id, inventor_id));
                CREATE TABLE patent_company (patent_id TEXT, company_id TEXT, PRIMARY KEY (patent_id, company_id));
            """)
            print(" Created tables using hardcoded schema")
        conn.commit()

    # Build each table if not already completed
    build_patents(conn, checkpoint)
    build_inventors(conn, checkpoint)
    build_companies(conn, checkpoint)
    build_patent_inventor(conn, checkpoint)
    build_patent_company(conn, checkpoint)

    # Run queries and reports (always run, even if tables were already built)
    results = run_queries(conn)
    generate_reports(conn, results)

    conn.close()
    print("\n Pipeline complete! Database saved to output/patent_pipeline.db")
    print("   Clean CSVs, reports, and JSON are in output/ folder.\n")
    # Optionally delete checkpoint file after successful completion
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("🧹 Removed checkpoint file (pipeline fully done).")

if __name__ == "__main__":
    main()