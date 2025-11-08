"""
Data Management Utilities and Performance Benchmarking
For Quantitative Trading System Database Infrastructure
"""

import asyncio
import concurrent.futures
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from database_manager import DatabaseConfig, DatabaseManager

logger = logging.getLogger(__name__)


# =====================================================
# Data Migration & Archival
# =====================================================


class DataArchiver:
    """
    Archives old data to S3/Parquet for cost-effective storage
    """

    def __init__(self, db_manager: DatabaseManager, archive_path: str = "/data/archive"):
        self.db = db_manager
        self.archive_path = Path(archive_path)
        self.archive_path.mkdir(parents=True, exist_ok=True)

    def archive_tick_data(
        self, date: datetime, symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Archive tick data to Parquet files

        Args:
            date: Date to archive
            symbols: Optional list of symbols (None = all)

        Returns:
            Archive statistics
        """
        stats = {
            "date": date.date(),
            "files_created": 0,
            "total_rows": 0,
            "compression_ratio": 0,
            "duration_seconds": 0,
        }

        start_time = time.time()

        # Query data from ClickHouse
        query = f"""
        SELECT *
        FROM ticks.trades
        WHERE toDate(timestamp) = %(date)s
        """

        if symbols:
            query += " AND symbol IN %(symbols)s"

        params = {"date": date.date()}
        if symbols:
            params["symbols"] = tuple(symbols)

        # Get data in chunks to manage memory
        client = self.db.clickhouse.get_client()
        result = client.execute(query, params, with_column_types=True)

        if result:
            columns = [col[0] for col in result[-1]]
            data = result[0]
            df = pd.DataFrame(data, columns=columns)

            if len(df) > 0:
                # Group by symbol for organized storage
                for symbol, group_df in df.groupby("symbol"):
                    # Create Parquet file path
                    file_path = (
                        self.archive_path / f"{date.strftime('%Y%m%d')}" / f"{symbol}.parquet"
                    )
                    file_path.parent.mkdir(parents=True, exist_ok=True)

                    # Convert to PyArrow table for better compression
                    table = pa.Table.from_pandas(group_df)

                    # Write with compression
                    pq.write_table(
                        table,
                        file_path,
                        compression="snappy",  # Good balance of speed/compression
                        use_dictionary=True,
                        compression_level=None,
                    )

                    stats["files_created"] += 1
                    stats["total_rows"] += len(group_df)

                # Calculate compression ratio
                original_size = df.memory_usage(deep=True).sum()
                compressed_size = sum(
                    f.stat().st_size
                    for f in (self.archive_path / date.strftime("%Y%m%d")).glob("*.parquet")
                )
                stats["compression_ratio"] = (
                    original_size / compressed_size if compressed_size > 0 else 0
                )

        stats["duration_seconds"] = time.time() - start_time

        logger.info(f"Archived {stats['total_rows']} rows in {stats['files_created']} files")
        return stats

    def restore_from_archive(self, date: datetime, symbols: List[str]) -> pd.DataFrame:
        """
        Restore data from Parquet archive

        Args:
            date: Date to restore
            symbols: List of symbols to restore

        Returns:
            Restored DataFrame
        """
        dfs = []
        date_path = self.archive_path / date.strftime("%Y%m%d")

        for symbol in symbols:
            file_path = date_path / f"{symbol}.parquet"
            if file_path.exists():
                df = pd.read_parquet(file_path)
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()


# =====================================================
# Data Quality Monitoring
# =====================================================


class DataQualityMonitor:
    """
    Monitors data quality and detects anomalies
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def check_data_completeness(self, symbol: str, date: datetime) -> Dict[str, Any]:
        """
        Check data completeness for a symbol/date

        Returns:
            Quality metrics dictionary
        """
        metrics = {
            "symbol": symbol,
            "date": date.date(),
            "expected_hours": 0,
            "actual_hours": 0,
            "missing_minutes": [],
            "gap_count": 0,
            "max_gap_seconds": 0,
            "outlier_count": 0,
            "completeness_score": 0.0,
        }

        # Get trading hours (simplified - adjust for actual exchange)
        market_open = datetime.combine(date.date(), datetime.strptime("09:30", "%H:%M").time())
        market_close = datetime.combine(date.date(), datetime.strptime("16:00", "%H:%M").time())

        metrics["expected_hours"] = (market_close - market_open).seconds / 3600

        # Query tick data
        query = """
        WITH tick_times AS (
            SELECT
                timestamp,
                price,
                LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
                LAG(price) OVER (ORDER BY timestamp) as prev_price
            FROM ticks.trades
            WHERE symbol = %(symbol)s
                AND timestamp >= %(start)s
                AND timestamp <= %(end)s
        )
        SELECT
            COUNT(*) as tick_count,
            COUNT(DISTINCT toStartOfMinute(timestamp)) as unique_minutes,
            MAX(timestamp - prev_timestamp) as max_gap,
            SUM(CASE WHEN timestamp - prev_timestamp > 60 THEN 1 ELSE 0 END) as gap_count,
            AVG(price) as avg_price,
            stddevPop(price) as std_price
        FROM tick_times
        """

        client = self.db.clickhouse.get_client()
        result = client.execute(
            query, {"symbol": symbol, "start": market_open, "end": market_close}
        )

        if result and result[0]:
            row = result[0]
            tick_count = row[0] or 0
            unique_minutes = row[1] or 0
            max_gap = row[2].total_seconds() if row[2] else 0
            gap_count = row[3] or 0
            avg_price = row[4] or 0
            std_price = row[5] or 0

            # Calculate metrics
            expected_minutes = metrics["expected_hours"] * 60
            metrics["actual_hours"] = unique_minutes / 60
            metrics["gap_count"] = gap_count
            metrics["max_gap_seconds"] = max_gap

            # Find missing minutes
            if unique_minutes < expected_minutes:
                all_minutes_query = """
                SELECT DISTINCT toStartOfMinute(timestamp) as minute
                FROM ticks.trades
                WHERE symbol = %(symbol)s
                    AND timestamp >= %(start)s
                    AND timestamp <= %(end)s
                ORDER BY minute
                """

                minutes_result = client.execute(
                    all_minutes_query, {"symbol": symbol, "start": market_open, "end": market_close}
                )

                if minutes_result:
                    actual_minutes = [m[0] for m in minutes_result]
                    expected_range = pd.date_range(start=market_open, end=market_close, freq="1min")
                    missing = set(expected_range) - set(actual_minutes)
                    metrics["missing_minutes"] = [m.isoformat() for m in sorted(missing)][
                        :10
                    ]  # First 10

            # Detect outliers (prices beyond 3 std dev)
            if std_price > 0:
                outlier_query = """
                SELECT COUNT(*) as outlier_count
                FROM ticks.trades
                WHERE symbol = %(symbol)s
                    AND timestamp >= %(start)s
                    AND timestamp <= %(end)s
                    AND ABS(price - %(avg_price)s) > %(threshold)s
                """

                outlier_result = client.execute(
                    outlier_query,
                    {
                        "symbol": symbol,
                        "start": market_open,
                        "end": market_close,
                        "avg_price": avg_price,
                        "threshold": 3 * std_price,
                    },
                )

                if outlier_result:
                    metrics["outlier_count"] = outlier_result[0][0]

            # Calculate completeness score
            minute_completeness = min(unique_minutes / expected_minutes, 1.0)
            gap_penalty = max(0, 1 - (gap_count / 100))  # Penalize gaps
            outlier_penalty = (
                max(0, 1 - (metrics["outlier_count"] / tick_count)) if tick_count > 0 else 1
            )

            metrics["completeness_score"] = (
                minute_completeness * 0.6 + gap_penalty * 0.2 + outlier_penalty * 0.2
            )

        return metrics

    def monitor_all_symbols(self, date: datetime, symbols: List[str]) -> pd.DataFrame:
        """
        Monitor data quality for all symbols

        Returns:
            DataFrame with quality metrics for each symbol
        """
        results = []

        for symbol in tqdm(symbols, desc="Checking data quality"):
            metrics = self.check_data_completeness(symbol, date)
            results.append(metrics)

        df = pd.DataFrame(results)

        # Store results in monitoring table
        for _, row in df.iterrows():
            self.db.clickhouse.get_client().execute(
                """
                INSERT INTO monitoring.data_quality VALUES
                """,
                [row.to_dict()],
            )

        return df


# =====================================================
# Performance Benchmarking
# =====================================================


class DatabaseBenchmark:
    """
    Benchmarks database performance for different operations
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.results = []

    def generate_mock_ticks(
        self, symbols: List[str], num_ticks: int = 1_000_000
    ) -> List[Dict[str, Any]]:
        """Generate mock tick data for testing"""
        ticks = []
        base_time = datetime.now() - timedelta(hours=1)

        for i in range(num_ticks):
            symbol = np.random.choice(symbols)
            tick = {
                "timestamp": base_time + timedelta(seconds=i * 0.001),
                "exchange": "TEST",
                "symbol": symbol,
                "price": np.random.uniform(100, 200),
                "volume": np.random.randint(100, 10000),
                "side": np.random.choice(["buy", "sell"]),
                "trade_id": f"test_{i}",
            }
            ticks.append(tick)

        return ticks

    def benchmark_insert_performance(
        self, batch_sizes: List[int] = [1000, 10000, 100000]
    ) -> pd.DataFrame:
        """Benchmark insert performance with different batch sizes"""
        results = []

        for batch_size in batch_sizes:
            # Generate test data
            ticks = self.generate_mock_ticks(["TEST1", "TEST2", "TEST3"], batch_size)

            # Benchmark ClickHouse insert
            start = time.time()
            self.db.clickhouse.insert_ticks(ticks, table="ticks.trades")
            ch_duration = time.time() - start

            results.append(
                {
                    "operation": "insert",
                    "database": "clickhouse",
                    "batch_size": batch_size,
                    "duration_seconds": ch_duration,
                    "throughput_per_sec": batch_size / ch_duration,
                    "latency_ms": (ch_duration / batch_size) * 1000,
                }
            )

            logger.info(f"ClickHouse insert {batch_size} ticks: {ch_duration:.2f}s")

        return pd.DataFrame(results)

    def benchmark_query_performance(self) -> pd.DataFrame:
        """Benchmark query performance for common operations"""
        results = []
        test_symbol = "AAPL"

        # Test queries
        queries = [
            {
                "name": "latest_100_ticks",
                "func": lambda: self.db.clickhouse.get_latest_ticks(test_symbol, 100),
            },
            {
                "name": "hourly_ohlcv",
                "func": lambda: self.db.clickhouse.query_ohlcv(
                    test_symbol, datetime.now() - timedelta(hours=24), datetime.now(), "1h"
                ),
            },
            {
                "name": "daily_vwap",
                "func": lambda: self.db.clickhouse.calculate_vwap(
                    test_symbol, datetime.now() - timedelta(days=1), datetime.now()
                ),
            },
            {
                "name": "microstructure_metrics",
                "func": lambda: self.db.clickhouse.get_market_microstructure(
                    test_symbol, datetime.now()
                ),
            },
        ]

        # Run benchmarks
        for query_info in queries:
            durations = []

            # Run multiple times for average
            for _ in range(10):
                start = time.time()
                result = query_info["func"]()
                duration = time.time() - start
                durations.append(duration)

            results.append(
                {
                    "operation": "query",
                    "query_name": query_info["name"],
                    "avg_duration_ms": np.mean(durations) * 1000,
                    "min_duration_ms": np.min(durations) * 1000,
                    "max_duration_ms": np.max(durations) * 1000,
                    "std_duration_ms": np.std(durations) * 1000,
                }
            )

            logger.info(f"Query {query_info['name']}: avg={np.mean(durations) * 1000:.2f}ms")

        return pd.DataFrame(results)

    def benchmark_concurrent_operations(
        self, num_workers: int = 10, operations_per_worker: int = 100
    ) -> Dict[str, Any]:
        """Benchmark concurrent read/write operations"""

        def worker_task(worker_id: int) -> Dict[str, Any]:
            """Task for each worker"""
            results = {
                "worker_id": worker_id,
                "operations": operations_per_worker,
                "duration": 0,
                "errors": 0,
            }

            start = time.time()

            for i in range(operations_per_worker):
                try:
                    if i % 2 == 0:
                        # Read operation
                        self.db.clickhouse.get_latest_ticks("AAPL", 10)
                    else:
                        # Write operation
                        tick = {
                            "timestamp": datetime.now(),
                            "exchange": "TEST",
                            "symbol": f"TEST{worker_id}",
                            "price": 100.0 + i,
                            "volume": 1000,
                            "side": "buy",
                            "trade_id": f"worker_{worker_id}_{i}",
                        }
                        self.db.clickhouse.insert_ticks([tick])
                except Exception as e:
                    results["errors"] += 1
                    logger.error(f"Worker {worker_id} error: {e}")

            results["duration"] = time.time() - start
            return results

        # Run concurrent workers
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker_task, i) for i in range(num_workers)]
            worker_results = [f.result() for f in futures]

        total_duration = time.time() - start

        # Aggregate results
        total_operations = sum(r["operations"] for r in worker_results)
        total_errors = sum(r["errors"] for r in worker_results)

        return {
            "num_workers": num_workers,
            "operations_per_worker": operations_per_worker,
            "total_operations": total_operations,
            "total_duration": total_duration,
            "throughput_per_sec": total_operations / total_duration,
            "total_errors": total_errors,
            "error_rate": total_errors / total_operations if total_operations > 0 else 0,
            "worker_results": worker_results,
        }

    def run_full_benchmark(self) -> Dict[str, Any]:
        """Run complete benchmark suite"""
        logger.info("Starting full database benchmark...")

        results = {
            "timestamp": datetime.now().isoformat(),
            "system_info": self.get_system_info(),
            "benchmarks": {},
        }

        # Insert performance
        logger.info("Benchmarking insert performance...")
        insert_df = self.benchmark_insert_performance()
        results["benchmarks"]["insert"] = insert_df.to_dict("records")

        # Query performance
        logger.info("Benchmarking query performance...")
        query_df = self.benchmark_query_performance()
        results["benchmarks"]["query"] = query_df.to_dict("records")

        # Concurrent operations
        logger.info("Benchmarking concurrent operations...")
        concurrent = self.benchmark_concurrent_operations()
        results["benchmarks"]["concurrent"] = concurrent

        # Storage metrics
        logger.info("Collecting storage metrics...")
        storage = self.get_storage_metrics()
        results["storage_metrics"] = storage

        return results

    def get_system_info(self) -> Dict[str, Any]:
        """Get system information for benchmark context"""
        return {
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
            "disk_usage_percent": psutil.disk_usage("/").percent,
        }

    def get_storage_metrics(self) -> Dict[str, Any]:
        """Get storage metrics from databases"""
        metrics = {}

        # ClickHouse storage
        ch_query = """
        SELECT
            database,
            table,
            sum(bytes) as size_bytes,
            sum(rows) as total_rows,
            sum(data_compressed_bytes) as compressed_bytes,
            sum(data_uncompressed_bytes) as uncompressed_bytes
        FROM system.parts
        WHERE active
        GROUP BY database, table
        ORDER BY size_bytes DESC
        """

        ch_result = self.db.clickhouse.get_client().execute(ch_query)

        ch_tables = []
        for row in ch_result:
            compression_ratio = row[5] / row[4] if row[4] > 0 else 1
            ch_tables.append(
                {
                    "database": row[0],
                    "table": row[1],
                    "size_mb": row[2] / (1024**2),
                    "rows": row[3],
                    "compression_ratio": compression_ratio,
                }
            )

        metrics["clickhouse"] = {
            "tables": ch_tables[:10],  # Top 10 tables
            "total_size_gb": sum(t["size_mb"] for t in ch_tables) / 1024,
            "total_rows": sum(t["rows"] for t in ch_tables),
        }

        # TimescaleDB storage
        ts_query = """
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
            pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 10
        """

        with self.db.timescale.get_session() as session:
            ts_result = pd.read_sql(ts_query, session.bind)

        metrics["timescaledb"] = {
            "tables": ts_result.to_dict("records"),
            "total_size_gb": ts_result["size_bytes"].sum() / (1024**3),
        }

        return metrics


# =====================================================
# Backup and Recovery
# =====================================================


class BackupManager:
    """
    Manages database backups and recovery
    """

    def __init__(self, db_manager: DatabaseManager, backup_path: str = "/data/backups"):
        self.db = db_manager
        self.backup_path = Path(backup_path)
        self.backup_path.mkdir(parents=True, exist_ok=True)

    def backup_clickhouse(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Backup ClickHouse tables

        Args:
            tables: List of tables to backup (None = all)

        Returns:
            Backup statistics
        """
        stats = {
            "timestamp": datetime.now().isoformat(),
            "tables_backed_up": [],
            "total_size_mb": 0,
            "duration_seconds": 0,
        }

        start = time.time()

        # Get list of tables if not specified
        if tables is None:
            query = """
            SELECT database, name
            FROM system.tables
            WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
            """
            result = self.db.clickhouse.get_client().execute(query)
            tables = [f"{row[0]}.{row[1]}" for row in result]

        # Backup each table
        for table in tables:
            backup_file = (
                self.backup_path
                / f"clickhouse_{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            )

            # Use clickhouse-client to dump table
            cmd = [
                "clickhouse-client",
                "--host",
                self.db.config.clickhouse_host,
                "--port",
                str(self.db.config.clickhouse_port),
                "--user",
                self.db.config.clickhouse_user,
                "--password",
                self.db.config.clickhouse_password,
                "--query",
                f"SELECT * FROM {table} FORMAT Native",
            ]

            try:
                with open(backup_file, "wb") as f:
                    subprocess.run(cmd, stdout=f, check=True)

                file_size_mb = backup_file.stat().st_size / (1024**2)
                stats["tables_backed_up"].append(table)
                stats["total_size_mb"] += file_size_mb

                logger.info(f"Backed up {table}: {file_size_mb:.2f} MB")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to backup {table}: {e}")

        stats["duration_seconds"] = time.time() - start

        # Write metadata
        metadata_file = (
            self.backup_path / f"metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(metadata_file, "w") as f:
            json.dump(stats, f, indent=2)

        return stats

    def backup_timescaledb(self) -> Dict[str, Any]:
        """Backup TimescaleDB using pg_dump"""
        stats = {
            "timestamp": datetime.now().isoformat(),
            "database": self.db.config.timescale_database,
            "size_mb": 0,
            "duration_seconds": 0,
        }

        start = time.time()

        backup_file = (
            self.backup_path / f"timescale_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql.gz"
        )

        # Use pg_dump with compression
        cmd = [
            "pg_dump",
            "-h",
            self.db.config.timescale_host,
            "-p",
            str(self.db.config.timescale_port),
            "-U",
            self.db.config.timescale_user,
            "-d",
            self.db.config.timescale_database,
            "--no-password",
            "--compress",
            "9",
            "-f",
            str(backup_file),
        ]

        # Set password via environment
        env = os.environ.copy()
        env["PGPASSWORD"] = self.db.config.timescale_password

        try:
            subprocess.run(cmd, env=env, check=True)
            stats["size_mb"] = backup_file.stat().st_size / (1024**2)
            stats["duration_seconds"] = time.time() - start

            logger.info(f"TimescaleDB backup completed: {stats['size_mb']:.2f} MB")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to backup TimescaleDB: {e}")
            raise

        return stats


# =====================================================
# Health Monitoring
# =====================================================


class HealthMonitor:
    """
    Monitors database health and performance
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        health = {"timestamp": datetime.now().isoformat(), "status": "healthy", "checks": {}}

        # Check ClickHouse
        try:
            ch_client = self.db.clickhouse.get_client()
            result = ch_client.execute("SELECT 1")

            # Get metrics
            metrics = ch_client.execute("""
                SELECT
                    (SELECT count() FROM system.processes) as active_queries,
                    (SELECT sum(memory_usage) FROM system.processes) as memory_used,
                    (SELECT max(elapsed) FROM system.processes) as max_query_time
            """)[0]

            health["checks"]["clickhouse"] = {
                "status": "healthy",
                "active_queries": metrics[0],
                "memory_used_mb": metrics[1] / (1024**2) if metrics[1] else 0,
                "max_query_seconds": metrics[2] if metrics[2] else 0,
            }
        except Exception as e:
            health["checks"]["clickhouse"] = {"status": "unhealthy", "error": str(e)}
            health["status"] = "degraded"

        # Check TimescaleDB
        try:
            with self.db.timescale.get_session() as session:
                result = session.execute("SELECT 1")

                # Get connection stats
                conn_stats = pd.read_sql(
                    """
                    SELECT
                        count(*) as total_connections,
                        count(*) FILTER (WHERE state = 'active') as active,
                        count(*) FILTER (WHERE state = 'idle') as idle,
                        max(EXTRACT(epoch FROM (now() - query_start))) as max_query_seconds
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                """,
                    session.bind,
                )

                health["checks"]["timescaledb"] = {
                    "status": "healthy",
                    "connections": conn_stats.iloc[0].to_dict(),
                }
        except Exception as e:
            health["checks"]["timescaledb"] = {"status": "unhealthy", "error": str(e)}
            health["status"] = "degraded"

        # Check Redis
        try:
            info = self.db.redis.client.info()
            health["checks"]["redis"] = {
                "status": "healthy",
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_mb": info.get("used_memory", 0) / (1024**2),
                "ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
            }
        except Exception as e:
            health["checks"]["redis"] = {"status": "unhealthy", "error": str(e)}
            health["status"] = "degraded"

        return health


# =====================================================
# Main Utility Runner
# =====================================================


async def main():
    """Main utility demonstration"""

    # Initialize database manager
    config = DatabaseConfig()
    db = DatabaseManager(config)
    await db.initialize()

    # Run health check
    health_monitor = HealthMonitor(db)
    health = health_monitor.check_health()
    print("\n=== Database Health ===")
    print(json.dumps(health, indent=2))

    # Run benchmark
    benchmark = DatabaseBenchmark(db)
    print("\n=== Running Benchmarks ===")
    results = benchmark.run_full_benchmark()

    # Print results
    print("\n=== Benchmark Results ===")
    print(
        f"System: {results['system_info']['cpu_count']} CPUs, "
        f"{results['system_info']['memory_total_gb']:.1f} GB RAM"
    )

    if "insert" in results["benchmarks"]:
        insert_df = pd.DataFrame(results["benchmarks"]["insert"])
        print("\nInsert Performance:")
        print(insert_df[["batch_size", "throughput_per_sec", "latency_ms"]])

    if "query" in results["benchmarks"]:
        query_df = pd.DataFrame(results["benchmarks"]["query"])
        print("\nQuery Performance:")
        print(query_df[["query_name", "avg_duration_ms"]])

    if "concurrent" in results["benchmarks"]:
        concurrent = results["benchmarks"]["concurrent"]
        print(f"\nConcurrent Operations:")
        print(f"  Throughput: {concurrent['throughput_per_sec']:.0f} ops/sec")
        print(f"  Error rate: {concurrent['error_rate']:.2%}")

    # Check data quality
    quality_monitor = DataQualityMonitor(db)
    quality = quality_monitor.check_data_completeness("AAPL", datetime.now())
    print("\n=== Data Quality Check ===")
    print(f"Symbol: {quality['symbol']}")
    print(f"Completeness Score: {quality['completeness_score']:.2%}")
    print(f"Gap Count: {quality['gap_count']}")
    print(f"Outlier Count: {quality['outlier_count']}")

    # Create backup
    backup_manager = BackupManager(db)
    print("\n=== Creating Backup ===")
    backup_stats = backup_manager.backup_clickhouse(["ticks.trades"])
    print(f"Backed up {len(backup_stats['tables_backed_up'])} tables")
    print(f"Total size: {backup_stats['total_size_mb']:.2f} MB")

    # Close connections
    db.close()
    print("\n=== Complete ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    asyncio.run(main())
