"""
Database Access Layer with MySQL Support (via PyMySQL).
Provides connection management, schema initialization, deduplicated job insertion,
run history tracking, and schema drift telemetry.
Includes an automatic fallback mode if MySQL server is not locally running.
"""

import os
import json
import sqlite3
import pymysql
from typing import List, Optional, Dict, Any
from datetime import datetime
from models import NormalizedJob, SchemaDriftReport

# MySQL Configuration from Environment Variables
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "acdyon_jobs")

DB_BACKEND = "mysql"  # 'mysql' or fallback 'sqlite'


def get_mysql_connection():
    """Attempts to connect to MySQL database."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


def ensure_mysql_database_exists():
    """Connects to MySQL server without database specified to create database if missing."""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4"
    )
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    conn.close()


def init_db():
    """Initializes tables in MySQL (with fallback to SQLite if MySQL server is unreachable)."""
    global DB_BACKEND
    try:
        ensure_mysql_database_exists()
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            # Jobs Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id VARCHAR(64) PRIMARY KEY,
                    source VARCHAR(64) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    company VARCHAR(255) NOT NULL,
                    location VARCHAR(255) NOT NULL,
                    description_snippet TEXT NOT NULL,
                    url VARCHAR(512) NOT NULL,
                    salary VARCHAR(128),
                    tags_json JSON,
                    published_date VARCHAR(64),
                    ingested_at VARCHAR(64) NOT NULL,
                    INDEX idx_source (source),
                    INDEX idx_company (company),
                    INDEX idx_ingested (ingested_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Ingestion Runs History Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id VARCHAR(64) PRIMARY KEY,
                    started_at VARCHAR(64) NOT NULL,
                    completed_at VARCHAR(64),
                    primary_source VARCHAR(64) NOT NULL,
                    fallback_used TINYINT(1) DEFAULT 0,
                    status VARCHAR(32) NOT NULL,
                    items_extracted INT DEFAULT 0,
                    items_saved INT DEFAULT 0,
                    items_skipped_dup INT DEFAULT 0,
                    error_message TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Schema Drift Telemetry Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_drifts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    source_name VARCHAR(64) NOT NULL,
                    drift_score FLOAT NOT NULL,
                    missing_fields_json JSON,
                    sample_snippet TEXT,
                    detected_at VARCHAR(64) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
        conn.close()
        DB_BACKEND = "mysql"
        print(f"[Database] Successfully connected to MySQL ({MYSQL_DATABASE}@{MYSQL_HOST}:{MYSQL_PORT}).")
    except Exception as e:
        print(f"[Database Warning] MySQL connection failed ({str(e)}). Switching to local fallback SQLite store.")
        DB_BACKEND = "sqlite"
        _init_sqlite_fallback()


def _init_sqlite_fallback():
    """Initializes local SQLite tables if MySQL is not running on host."""
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT NOT NULL,
                description_snippet TEXT NOT NULL,
                url TEXT NOT NULL,
                salary TEXT,
                tags_json TEXT,
                published_date TEXT,
                ingested_at TEXT NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                primary_source TEXT NOT NULL,
                fallback_used INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                items_extracted INTEGER DEFAULT 0,
                items_saved INTEGER DEFAULT 0,
                items_skipped_dup INTEGER DEFAULT 0,
                error_message TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_drifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                drift_score REAL NOT NULL,
                missing_fields_json TEXT,
                sample_snippet TEXT,
                detected_at TEXT NOT NULL
            );
        """)
        conn.commit()


def save_jobs(jobs: List[NormalizedJob]) -> Dict[str, int]:
    """
    Inserts jobs into MySQL (or fallback SQLite), ignoring duplicates based on canonical primary key ID.
    """
    if not jobs:
        return {"inserted": 0, "duplicates": 0}

    inserted = 0
    duplicates = 0

    if DB_BACKEND == "mysql":
        try:
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                for job in jobs:
                    tags_payload = json.dumps(job.tags)
                    query = """
                        INSERT IGNORE INTO jobs (
                            id, source, title, company, location,
                            description_snippet, url, salary, tags_json,
                            published_date, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    affected = cursor.execute(query, (
                        job.id, job.source, job.title, job.company, job.location,
                        job.description_snippet, job.url, job.salary, tags_payload,
                        job.published_date, job.ingested_at
                    ))
                    if affected > 0:
                        inserted += 1
                    else:
                        duplicates += 1
            conn.close()
            return {"inserted": inserted, "duplicates": duplicates}
        except Exception as e:
            print(f"[MySQL Insert Error]: {e}")

    # Fallback SQLite insert
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        cursor = conn.cursor()
        for job in jobs:
            try:
                cursor.execute("""
                    INSERT INTO jobs (
                        id, source, title, company, location,
                        description_snippet, url, salary, tags_json,
                        published_date, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job.id, job.source, job.title, job.company, job.location,
                    job.description_snippet, job.url, job.salary, json.dumps(job.tags),
                    job.published_date, job.ingested_at
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        conn.commit()
    return {"inserted": inserted, "duplicates": duplicates}


def get_all_jobs(limit: int = 100, source: Optional[str] = None, search: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve jobs from MySQL or fallback SQLite."""
    if DB_BACKEND == "mysql":
        try:
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                query = "SELECT * FROM jobs WHERE 1=1"
                params = []
                if source and source != "ALL":
                    query += " AND source = %s"
                    params.append(source)
                if search:
                    query += " AND (title LIKE %s OR company LIKE %s OR tags_json LIKE %s)"
                    wildcard = f"%{search}%"
                    params.extend([wildcard, wildcard, wildcard])
                query += " ORDER BY ingested_at DESC LIMIT %s"
                params.append(limit)

                cursor.execute(query, params)
                rows = cursor.fetchall()
                for r in rows:
                    if isinstance(r.get("tags_json"), str):
                        r["tags"] = json.loads(r["tags_json"])
                    elif isinstance(r.get("tags_json"), list):
                        r["tags"] = r["tags_json"]
                    else:
                        r["tags"] = []
                conn.close()
                return rows
        except Exception as e:
            print(f"[MySQL Query Error]: {e}")

    # Fallback SQLite query
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = "SELECT * FROM jobs WHERE 1=1"
        params = []
        if source and source != "ALL":
            query += " AND source = ?"
            params.append(source)
        if search:
            query += " AND (title LIKE ? OR company LIKE ? OR tags_json LIKE ?)"
            wildcard = f"%{search}%"
            params.extend([wildcard, wildcard, wildcard])
        query += " ORDER BY ingested_at DESC LIMIT ?"
        params.append(limit)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["tags"] = json.loads(d["tags_json"]) if d.get("tags_json") else []
            result.append(d)
        return result


def record_run(run_id: str, primary_source: str, status: str, started_at: str,
               completed_at: Optional[str] = None, fallback_used: bool = False,
               items_extracted: int = 0, items_saved: int = 0, items_skipped_dup: int = 0,
               error_message: Optional[str] = None):
    """Log execution run metrics into MySQL or SQLite."""
    if DB_BACKEND == "mysql":
        try:
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO ingestion_runs (
                        run_id, started_at, completed_at, primary_source, fallback_used,
                        status, items_extracted, items_saved, items_skipped_dup, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        completed_at = VALUES(completed_at),
                        status = VALUES(status),
                        items_extracted = VALUES(items_extracted),
                        items_saved = VALUES(items_saved),
                        items_skipped_dup = VALUES(items_skipped_dup),
                        error_message = VALUES(error_message)
                """, (
                    run_id, started_at, completed_at, primary_source, 1 if fallback_used else 0,
                    status, items_extracted, items_saved, items_skipped_dup, error_message
                ))
            conn.close()
            return
        except Exception as e:
            print(f"[MySQL Record Run Error]: {e}")

    # Fallback SQLite
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO ingestion_runs (
                run_id, started_at, completed_at, primary_source, fallback_used,
                status, items_extracted, items_saved, items_skipped_dup, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, started_at, completed_at, primary_source, 1 if fallback_used else 0,
            status, items_extracted, items_saved, items_skipped_dup, error_message
        ))
        conn.commit()


def record_schema_drift(report: SchemaDriftReport):
    """Log schema drift anomaly to MySQL or SQLite."""
    if DB_BACKEND == "mysql":
        try:
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO schema_drifts (source_name, drift_score, missing_fields_json, sample_snippet, detected_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    report.source_name,
                    report.drift_score,
                    json.dumps(report.missing_fields),
                    report.sample_payload_snippet,
                    report.detected_at
                ))
            conn.close()
            return
        except Exception as e:
            print(f"[MySQL Record Drift Error]: {e}")

    # Fallback SQLite
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO schema_drifts (source_name, drift_score, missing_fields_json, sample_snippet, detected_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            report.source_name,
            report.drift_score,
            json.dumps(report.missing_fields),
            report.sample_payload_snippet,
            report.detected_at
        ))
        conn.commit()


def get_stats() -> Dict[str, Any]:
    """Retrieve overall database statistics."""
    if DB_BACKEND == "mysql":
        try:
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as cnt FROM jobs;")
                total_jobs = cursor.fetchone()["cnt"]

                cursor.execute("SELECT COUNT(DISTINCT source) as cnt FROM jobs;")
                total_sources = cursor.fetchone()["cnt"]

                cursor.execute("SELECT COUNT(*) as cnt FROM ingestion_runs;")
                total_runs = cursor.fetchone()["cnt"]

                cursor.execute("SELECT COUNT(*) as cnt FROM schema_drifts;")
                total_drifts = cursor.fetchone()["cnt"]

                conn.close()
                return {
                    "backend": "MySQL",
                    "total_jobs": total_jobs,
                    "total_sources": total_sources,
                    "total_runs": total_runs,
                    "total_drifts": total_drifts
                }
        except Exception as e:
            print(f"[MySQL Stats Error]: {e}")

    # Fallback SQLite stats
    with sqlite3.connect("fallback_jobs.sqlite3") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs;")
        total_jobs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT source) FROM jobs;")
        total_sources = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM ingestion_runs;")
        total_runs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM schema_drifts;")
        total_drifts = cursor.fetchone()[0]

        return {
            "backend": "SQLite (Fallback)",
            "total_jobs": total_jobs,
            "total_sources": total_sources,
            "total_runs": total_runs,
            "total_drifts": total_drifts
        }


# Initialize tables on startup
init_db()
