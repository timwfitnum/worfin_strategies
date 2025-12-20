#!/usr/bin/env python3
"""
Database Setup Script for Quantitative Trading System.
Initializes TimescaleDB and ClickHouse schemas.
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import asyncpg
import clickhouse_driver
from clickhouse_driver import Client as ClickHouseClient

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def wait_for_postgres(
    host: str, port: int, user: str, password: str, max_retries: int = 30
) -> bool:
    """Wait for PostgreSQL to be ready."""
    import psycopg2

    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname="postgres",
                connect_timeout=5,
            )
            conn.close()
            logger.info("PostgreSQL is ready")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"Waiting for PostgreSQL... ({i + 1}/{max_retries})")
            time.sleep(2)

    logger.error("PostgreSQL did not become ready in time")
    return False


def wait_for_clickhouse(host: str, port: int, max_retries: int = 30) -> bool:
    """Wait for ClickHouse to be ready."""
    for i in range(max_retries):
        try:
            client = ClickHouseClient(host=host, port=port)
            client.execute("SELECT 1")
            logger.info("ClickHouse is ready")
            return True
        except Exception as e:
            logger.warning(f"Waiting for ClickHouse... ({i + 1}/{max_retries})")
            time.sleep(2)

    logger.error("ClickHouse did not become ready in time")
    return False


def setup_timescaledb(settings) -> bool:
    """Initialize TimescaleDB schema."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    db_config = settings.database.timescale

    try:
        # First connect to postgres database to create our database
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password.get_secret_value(),
            dbname="postgres",
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cur:
            # Check if database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_config.database,))
            if not cur.fetchone():
                logger.info(f"Creating database: {db_config.database}")
                cur.execute(f"CREATE DATABASE {db_config.database}")
            else:
                logger.info(f"Database {db_config.database} already exists")

        conn.close()

        # Now connect to our database and run schema
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password.get_secret_value(),
            dbname=db_config.database,
        )

        # Read and execute schema file
        schema_path = Path(__file__).parent / "init_db.sql"
        if schema_path.exists():
            logger.info("Executing TimescaleDB schema...")
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            with conn.cursor() as cur:
                # Split by statements and execute individually
                # (handles multi-statement SQL better)
                statements = schema_sql.split(";")
                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt and not stmt.startswith("--"):
                        try:
                            cur.execute(stmt)
                            conn.commit()
                        except psycopg2.Error as e:
                            # Log but continue - some statements may already exist
                            if "already exists" not in str(e):
                                logger.warning(f"Statement warning: {e}")
                            conn.rollback()

            logger.info("TimescaleDB schema executed successfully")
        else:
            logger.warning(f"Schema file not found: {schema_path}")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"TimescaleDB setup failed: {e}")
        return False


def setup_clickhouse(settings) -> bool:
    """Initialize ClickHouse schema."""
    db_config = settings.database.clickhouse

    try:
        client = ClickHouseClient(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password.get_secret_value(),
        )

        # Read and execute schema file
        schema_path = Path(__file__).parent / "clickhouse_schema.sql"
        if schema_path.exists():
            logger.info("Executing ClickHouse schema...")
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            # Split by statements and execute individually
            statements = schema_sql.split(";")
            for stmt in statements:
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--") and not stmt.startswith("/*"):
                    try:
                        client.execute(stmt)
                    except Exception as e:
                        # Log but continue
                        if "already exists" not in str(e).lower():
                            logger.warning(f"Statement warning: {e}")

            logger.info("ClickHouse schema executed successfully")
        else:
            logger.warning(f"Schema file not found: {schema_path}")

        return True

    except Exception as e:
        logger.error(f"ClickHouse setup failed: {e}")
        return False


def verify_setup(settings) -> bool:
    """Verify database setup is complete."""
    import psycopg2

    db_config = settings.database
    success = True

    # Verify TimescaleDB
    try:
        conn = psycopg2.connect(
            host=db_config.timescale.host,
            port=db_config.timescale.port,
            user=db_config.timescale.user,
            password=db_config.timescale.password.get_secret_value(),
            dbname=db_config.timescale.database,
        )

        with conn.cursor() as cur:
            # Check schemas exist
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name IN ('market_data', 'reference', 'trading', 'analytics', 'monitoring')
            """)
            schemas = [row[0] for row in cur.fetchall()]
            logger.info(f"TimescaleDB schemas found: {schemas}")

            # Check hypertables
            cur.execute("""
                SELECT hypertable_name FROM timescaledb_information.hypertables
            """)
            hypertables = [row[0] for row in cur.fetchall()]
            logger.info(f"TimescaleDB hypertables: {hypertables}")

        conn.close()

    except Exception as e:
        logger.error(f"TimescaleDB verification failed: {e}")
        success = False

    # Verify ClickHouse
    try:
        client = ClickHouseClient(
            host=db_config.clickhouse.host,
            port=db_config.clickhouse.port,
            user=db_config.clickhouse.user,
            password=db_config.clickhouse.password.get_secret_value(),
        )

        # Check databases exist
        databases = client.execute("SHOW DATABASES")
        db_names = [row[0] for row in databases]
        expected_dbs = ["ticks", "ohlcv", "orderbook", "analytics", "monitoring"]
        found_dbs = [db for db in expected_dbs if db in db_names]
        logger.info(f"ClickHouse databases found: {found_dbs}")

        # Check tables in ticks database
        tables = client.execute("SHOW TABLES FROM ticks")
        table_names = [row[0] for row in tables]
        logger.info(f"ClickHouse ticks tables: {table_names}")

    except Exception as e:
        logger.error(f"ClickHouse verification failed: {e}")
        success = False

    return success


def main():
    """Main setup function."""
    logger.info("=" * 60)
    logger.info("Quantitative Trading System - Database Setup")
    logger.info("=" * 60)

    # Get settings
    settings = get_settings()
    db_config = settings.database

    # Wait for databases to be ready
    logger.info("Waiting for databases to be ready...")

    pg_ready = wait_for_postgres(
        host=db_config.timescale.host,
        port=db_config.timescale.port,
        user=db_config.timescale.user,
        password=db_config.timescale.password.get_secret_value(),
    )

    ch_ready = wait_for_clickhouse(
        host=db_config.clickhouse.host,
        port=db_config.clickhouse.port,
    )

    if not pg_ready or not ch_ready:
        logger.error("Databases not ready. Exiting.")
        sys.exit(1)

    # Setup databases
    logger.info("Setting up TimescaleDB...")
    ts_success = setup_timescaledb(settings)

    logger.info("Setting up ClickHouse...")
    ch_success = setup_clickhouse(settings)

    if not ts_success or not ch_success:
        logger.error("Database setup failed")
        sys.exit(1)

    # Verify setup
    logger.info("Verifying database setup...")
    if verify_setup(settings):
        logger.info("=" * 60)
        logger.info("Database setup completed successfully!")
        logger.info("=" * 60)
    else:
        logger.warning("Database setup completed with warnings")
        sys.exit(1)


if __name__ == "__main__":
    main()
