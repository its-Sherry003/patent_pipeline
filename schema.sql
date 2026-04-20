-- schema.sql
-- Patent Database Schema

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