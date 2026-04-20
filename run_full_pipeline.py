"""
Patent Data Pipeline – Full Local Processing
Reads TSV files from data/ folder, builds SQLite DB, runs queries, exports reports.
"""

import pandas as pd
import sqlite3
import zipfile
import json
import os
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================
DATA_DIR = Path("data")
DB_PATH = Path("output/patent_pipeline.db")
CHUNK_SIZE = 50000

# Ensure output directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================
# HELPER: Read a zipped TSV in chunks
# ============================================================
def read_tsv_in_chunks(zip_path, columns=None, chunksize=CHUNK_SIZE):
    """Generator that yields chunks of a zipped TSV file."""
    with zipfile.ZipFile(zip_path, 'r') as z:
        tsv_name = z.namelist()[0]
        with z.open(tsv_name) as f:
            chunk_iter = pd.read_csv(f, sep='\t', usecols=columns, chunksize=chunksize, low_memory=False)
            for chunk in chunk_iter:
                yield chunk

# ============================================================
# 1. BUILD PATENTS TABLE
# ============================================================
def build_patents(conn):
    print("Building patents table...")
    patents_data = []
    patent_abstracts = {}

    abstract_path = DATA_DIR / "g_patent_abstract.tsv.zip"
    if abstract_path.exists():
        for chunk in read_tsv_in_chunks(abstract_path, columns=['patent_id', 'patent_abstract']):
            for _, row in chunk.iterrows():
                patent_abstracts[row['patent_id']] = row['patent_abstract']
    else:
        print("Warning: g_patent_abstract.tsv.zip not found; abstracts will be empty.")

    patent_path = DATA_DIR / "g_patent.tsv.zip"
    if not patent_path.exists():
        raise FileNotFoundError("g_patent.tsv.zip not found in data/ folder")

    for chunk in read_tsv_in_chunks(patent_path, columns=['patent_id', 'patent_title', 'patent_date']):
        chunk = chunk.rename(columns={'patent_date': 'filing_date'})
        chunk['year'] = pd.to_datetime(chunk['filing_date'], errors='coerce').dt.year
        chunk['abstract'] = chunk['patent_id'].map(patent_abstracts).fillna("No abstract")
        chunk.to_sql('patents', conn, if_exists='append', index=False)
        print(f"  Inserted {len(chunk)} patents")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_patents_year ON patents(year);")
    print("Patents table done.\n")

# ============================================================
# 2. BUILD INVENTORS TABLE
# ============================================================
def build_inventors(conn):
    print("Building inventors table...")
    loc_map = {}
    loc_path = DATA_DIR / "g_location_disambiguated.tsv.zip"
    if loc_path.exists():
        for chunk in read_tsv_in_chunks(loc_path, columns=['location_id', 'disambig_country']):
            for _, row in chunk.iterrows():
                loc_map[row['location_id']] = row['disambig_country']
    else:
        print("Warning: location file missing; countries will be Unknown")

    inv_path = DATA_DIR / "g_inventor_disambiguated.tsv.zip"
    if not inv_path.exists():
        raise FileNotFoundError("g_inventor_disambiguated.tsv.zip not found")

    for chunk in read_tsv_in_chunks(inv_path, columns=['inventor_id', 'inventor_first_name', 'inventor_last_name', 'location_id']):
        chunk['name'] = chunk['inventor_first_name'].fillna('') + ' ' + chunk['inventor_last_name'].fillna('')
        chunk['name'] = chunk['name'].str.strip()
        chunk.loc[chunk['name'] == '', 'name'] = 'Unknown'
        chunk['country'] = chunk['location_id'].map(loc_map).fillna('Unknown')
        df_inv = chunk[['inventor_id', 'name', 'country']].drop_duplicates('inventor_id')
        df_inv.to_sql('inventors', conn, if_exists='append', index=False)
        print(f"  Inserted {len(df_inv)} inventors")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_inventors_id ON inventors(inventor_id);")
    print("Inventors table done.\n")

# ============================================================
# 3. BUILD COMPANIES TABLE
# ============================================================
def build_companies(conn):
    print("Building companies table...")
    loc_map = {}
    loc_path = DATA_DIR / "g_location_disambiguated.tsv.zip"
    if loc_path.exists():
        for chunk in read_tsv_in_chunks(loc_path, columns=['location_id', 'disambig_country']):
            for _, row in chunk.iterrows():
                loc_map[row['location_id']] = row['disambig_country']

    assignee_path = DATA_DIR / "g_persistent_assignee.tsv.zip"
    if not assignee_path.exists():
        assignee_path = DATA_DIR / "g_assignee_disambiguated.tsv.zip"
        if not assignee_path.exists():
            raise FileNotFoundError("No assignee file found (g_persistent_assignee or g_assignee_disambiguated)")

    for chunk in read_tsv_in_chunks(assignee_path, columns=['assignee_id', 'assignee_organization', 'location_id']):
        chunk = chunk.rename(columns={'assignee_organization': 'name'})
        chunk['country'] = chunk['location_id'].map(loc_map).fillna('Unknown')
        df_comp = chunk[['assignee_id', 'name']].drop_duplicates('assignee_id')
        df_comp = df_comp.rename(columns={'assignee_id': 'company_id'})
        df_comp.to_sql('companies', conn, if_exists='append', index=False)
        print(f"  Inserted {len(df_comp)} companies")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_id ON companies(company_id);")
    print("Companies table done.\n")

# ============================================================
# 4. BUILD PATENT-INVENTOR RELATION
# ============================================================
def build_patent_inventor(conn):
    print("Building patent_inventor relationship...")
    rel_path = DATA_DIR / "g_inventor_not_disambiguated.tsv.zip"
    if not rel_path.exists():
        raise FileNotFoundError("g_inventor_not_disambiguated.tsv.zip not found")

    for chunk in read_tsv_in_chunks(rel_path, columns=['patent_id', 'inventor_id']):
        chunk = chunk.drop_duplicates()
        chunk.to_sql('patent_inventor', conn, if_exists='append', index=False)
        print(f"  Inserted {len(chunk)} patent-inventor links")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_patent ON patent_inventor(patent_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_inventor ON patent_inventor(inventor_id);")
    print("Patent-inventor table done.\n")

# ============================================================
# 5. BUILD PATENT-COMPANY RELATION
# ============================================================
def build_patent_company(conn):
    print("Building patent_company relationship...")
    rel_path = DATA_DIR / "g_assignee_not_disambiguated.tsv.zip"
    if not rel_path.exists():
        raise FileNotFoundError("g_assignee_not_disambiguated.tsv.zip not found")

    for chunk in read_tsv_in_chunks(rel_path, columns=['patent_id', 'assignee_id']):
        chunk = chunk.drop_duplicates()
        chunk = chunk.rename(columns={'assignee_id': 'company_id'})
        chunk.to_sql('patent_company', conn, if_exists='append', index=False)
        print(f"  Inserted {len(chunk)} patent-company links")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_patent ON patent_company(patent_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_company ON patent_company(company_id);")
    print("Patent-company table done.\n")

# ============================================================
# 6. RUN REQUIRED SQL QUERIES
# ============================================================
def run_queries(conn):
    print("\n" + "="*60)
    print("RUNNING REQUIRED SQL QUERIES")
    print("="*60)

    queries = {
        "Q1_Top_Inventors": """
            SELECT i.name, COUNT(pi.patent_id) as patent_count
            FROM inventors i
            JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id
            GROUP BY i.inventor_id
            ORDER BY patent_count DESC
            LIMIT 10
        """,
        "Q2_Top_Companies": """
            SELECT c.name, COUNT(pc.patent_id) as patent_count
            FROM companies c
            JOIN patent_company pc ON c.company_id = pc.company_id
            GROUP BY c.company_id
            ORDER BY patent_count DESC
            LIMIT 10
        """,
        "Q3_Top_Countries": """
            SELECT i.country, COUNT(DISTINCT pi.patent_id) as patent_count
            FROM inventors i
            JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id
            GROUP BY i.country
            ORDER BY patent_count DESC
            LIMIT 10
        """,
        "Q4_Trends_Over_Time": """
            SELECT year, COUNT(*) as patent_count
            FROM patents
            WHERE year IS NOT NULL
            GROUP BY year
            ORDER BY year
        """,
        "Q5_Join_Query": """
            SELECT p.patent_id, p.title, i.name as inventor_name, c.name as company_name
            FROM patents p
            LEFT JOIN patent_inventor pi ON p.patent_id = pi.patent_id
            LEFT JOIN inventors i ON pi.inventor_id = i.inventor_id
            LEFT JOIN patent_company pc ON p.patent_id = pc.patent_id
            LEFT JOIN companies c ON pc.company_id = c.company_id
            LIMIT 20
        """,
        "Q6_CTE_Query": """
            WITH yearly_inventor_counts AS (
                SELECT i.inventor_id, i.name, strftime('%Y', p.filing_date) as year, COUNT(*) as cnt
                FROM inventors i
                JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id
                JOIN patents p ON pi.patent_id = p.patent_id
                GROUP BY i.inventor_id, year
            )
            SELECT name, year, cnt
            FROM yearly_inventor_counts
            WHERE cnt > 1
            ORDER BY year DESC, cnt DESC
            LIMIT 20
        """,
        "Q7_Ranking_Query": """
            SELECT i.name, COUNT(pi.patent_id) as patent_count,
                   RANK() OVER (ORDER BY COUNT(pi.patent_id) DESC) as rank
            FROM inventors i
            JOIN patent_inventor pi ON i.inventor_id = pi.inventor_id
            GROUP BY i.inventor_id
            ORDER BY rank
            LIMIT 20
        """
    }

    results = {}
    for name, sql in queries.items():
        print(f"\n--- {name} ---")
        df = pd.read_sql_query(sql, conn)
        print(df.to_string(index=False))
        results[name] = df
    return results

# ============================================================
# 7. GENERATE REPORTS (CSV, JSON, clean CSVs)
# ============================================================
def generate_reports(conn, results):
    print("\n" + "="*60)
    print("EXPORTING REPORTS")
    print("="*60)

    # CSV reports (required at least two; we export three)
    results["Q1_Top_Inventors"].to_csv("output/top_inventors.csv", index=False)
    results["Q2_Top_Companies"].to_csv("output/top_companies.csv", index=False)
    results["Q3_Top_Countries"].to_csv("output/country_trends.csv", index=False)
    print("Exported: top_inventors.csv, top_companies.csv, country_trends.csv")

    # Clean CSVs (required by assignment)
    clean_patents = pd.read_sql_query("SELECT * FROM patents", conn)
    clean_inventors = pd.read_sql_query("SELECT * FROM inventors", conn)
    clean_companies = pd.read_sql_query("SELECT * FROM companies", conn)
    clean_patents.to_csv("output/clean_patents.csv", index=False)
    clean_inventors.to_csv("output/clean_inventors.csv", index=False)
    clean_companies.to_csv("output/clean_companies.csv", index=False)
    print("Exported clean CSVs: clean_patents.csv, clean_inventors.csv, clean_companies.csv")

    # JSON report
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
    print("Exported: report.json")

# ============================================================
# MAIN
# ============================================================
def main():
    # Create fresh database
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)

    # Create tables using schema.sql if exists, else hardcoded
    schema_path = Path("sql/schema.sql")
    if schema_path.exists():
        with open(schema_path, "r") as f:
            conn.executescript(f.read())
        print("Created tables from sql/schema.sql")
    else:
        conn.executescript("""
            CREATE TABLE patents (
                patent_id TEXT PRIMARY KEY,
                title TEXT,
                abstract TEXT,
                filing_date TEXT,
                year INTEGER
            );
            CREATE TABLE inventors (
                inventor_id TEXT PRIMARY KEY,
                name TEXT,
                country TEXT
            );
            CREATE TABLE companies (
                company_id TEXT PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE patent_inventor (
                patent_id TEXT,
                inventor_id TEXT,
                PRIMARY KEY (patent_id, inventor_id)
            );
            CREATE TABLE patent_company (
                patent_id TEXT,
                company_id TEXT,
                PRIMARY KEY (patent_id, company_id)
            );
        """)
        print("Created tables using hardcoded schema (sql/schema.sql not found)")
    conn.commit()

    # Build all tables
    build_patents(conn)
    build_inventors(conn)
    build_companies(conn)
    build_patent_inventor(conn)
    build_patent_company(conn)

    # Run queries and generate reports
    results = run_queries(conn)
    generate_reports(conn, results)

    conn.close()
    print("\n✅ Pipeline complete. Database saved to output/patent_pipeline.db")
    print("   Clean CSVs, reports, and JSON are in output/ folder.")

if __name__ == "__main__":
    main()