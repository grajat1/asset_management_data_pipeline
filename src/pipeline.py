"""Small Oracle-backed asset-management data pipeline prototype."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

INPUTS = {
    "security_master": ("security_master.csv", "SECURITY_MASTER_RAW"),
    "transactions": ("transactions.csv", "TRANSACTIONS_RAW"),
    "positions": ("positions.csv", "POSITIONS_RAW"),
    "market_prices": ("market_prices.csv", "MARKET_PRICES_RAW"),
    "restricted_securities": ("restricted_securities.csv", "RESTRICTED_SECURITIES_RAW"),
}

EXPECTED_COLUMNS = {
    "security_master": {"security_id", "isin", "asset_class", "currency", "effective_from", "effective_to"},
    "transactions": {"transaction_id", "portfolio_id", "security_id", "trade_date", "quantity", "price", "currency"},
    "positions": {"portfolio_id", "security_id", "position_date", "quantity", "book_value", "currency"},
    "market_prices": {"security_id", "price_date", "price", "price_currency", "price_source"},
    "restricted_securities": {"security_id", "restriction_type", "effective_from", "effective_to", "status"},
}


def connect() -> oracledb.Connection:
    """Connect using environment variables; never hard-code secrets."""
    required = ("DB_USER", "DB_PASSWORD", "DB_DSN")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Missing environment variables: " + ", ".join(missing)
        )
    # Import here so pure parsing/unit tests can run without Oracle installed.
    import oracledb

    return oracledb.connect(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dsn=os.environ["DB_DSN"],
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_date(value: str | None) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_decimal(value: str | None) -> Decimal | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def read_csv(path: Path, dataset: str) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError(f"{path.name}: missing CSV header")
        actual = {name.strip().lower() for name in reader.fieldnames}
        expected = EXPECTED_COLUMNS[dataset]
        if not expected.issubset(actual):
            raise ValueError(
                f"{path.name}: missing columns {sorted(expected - actual)}"
            )
        # Normalize keys to lowercase/trimmed names.
        return [
            {str(k).strip().lower(): (v.strip() if isinstance(v, str) else "")
             for k, v in row.items() if k is not None}
            for row in reader
        ]


def insert_run(cursor: Any, business_date: date) -> int:
    import oracledb
    run_id_var = cursor.var(oracledb.NUMBER)
    cursor.execute(
        """
        INSERT INTO PIPELINE_RUNS
          (BUSINESS_DATE, STATUS, STARTED_AT)
        VALUES (:business_date, 'RUNNING', SYSTIMESTAMP)
        RETURNING RUN_ID INTO :run_id
        """,
        {"business_date": business_date, "run_id": run_id_var},
    )
    return int(run_id_var.getvalue())


def register_delivery(
    cursor: Any, file_hash: str, file_name: str, run_id: int
) -> bool:
    """Return False if an identical delivery already succeeded."""
    cursor.execute(
        "SELECT STATUS FROM FILE_DELIVERIES WHERE FILE_HASH = :file_hash",
        {"file_hash": file_hash},
    )
    existing = cursor.fetchone()
    if existing and existing[0] == "SUCCEEDED":
        return False
    if existing:
        cursor.execute(
            """
            UPDATE FILE_DELIVERIES
            SET RUN_ID = :run_id, STATUS = 'PROCESSING',
                RECEIVED_AT = SYSTIMESTAMP, COMPLETED_AT = NULL
            WHERE FILE_HASH = :file_hash
            """,
            {"run_id": run_id, "file_hash": file_hash},
        )
    else:
        cursor.execute(
            """
            INSERT INTO FILE_DELIVERIES
              (FILE_HASH, FILE_NAME, RUN_ID, STATUS)
            VALUES (:file_hash, :file_name, :run_id, 'PROCESSING')
            """,
            {
                "file_hash": file_hash,
                "file_name": file_name,
                "run_id": run_id,
            },
        )
    return True


def raw_insert_sql(table: str) -> str:
    columns = {
        "SECURITY_MASTER_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, ISIN, "
            "ASSET_CLASS, CURRENCY, EFFECTIVE_FROM, EFFECTIVE_TO"
        ),
        "TRANSACTIONS_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, "
            "PORTFOLIO_ID, SECURITY_ID, TRADE_DATE, QUANTITY, PRICE, CURRENCY"
        ),
        "POSITIONS_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, PORTFOLIO_ID, SECURITY_ID, "
            "POSITION_DATE, QUANTITY, BOOK_VALUE, CURRENCY"
        ),
        "MARKET_PRICES_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, PRICE_DATE, "
            "PRICE, PRICE_CURRENCY, PRICE_SOURCE"
        ),
        "RESTRICTED_SECURITIES_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, "
            "RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO, STATUS"
        ),
    }
    if table not in columns:
        raise ValueError("Unsupported raw table")
    names = columns[table].split(", ")
    binds = ", ".join(f":{i}" for i in range(1, len(names) + 1))
    return f"INSERT INTO {table} ({columns[table]}) VALUES ({binds})"


def raw_values(
    dataset: str, row: dict[str, str], run_id: int, source_file: str, row_num: int
) -> tuple[Any, ...]:
    prefix = (run_id, source_file, row_num)
    if dataset == "security_master":
        return prefix + (
            row.get("security_id") or None, row.get("isin") or None,
            row.get("asset_class") or None, row.get("currency") or None,
            parse_date(row.get("effective_from")), parse_date(row.get("effective_to")),
        )
    if dataset == "transactions":
        return prefix + (
            row.get("transaction_id") or None, row.get("portfolio_id") or None,
            row.get("security_id") or None, parse_date(row.get("trade_date")),
            parse_decimal(row.get("quantity")), parse_decimal(row.get("price")),
            row.get("currency") or None,
        )
    if dataset == "positions":
        return prefix + (
            row.get("portfolio_id") or None, row.get("security_id") or None,
            parse_date(row.get("position_date")), parse_decimal(row.get("quantity")),
            parse_decimal(row.get("book_value")), row.get("currency") or None,
        )
    if dataset == "market_prices":
        return prefix + (
            row.get("security_id") or None, parse_date(row.get("price_date")),
            parse_decimal(row.get("price")), row.get("price_currency") or None,
            row.get("price_source") or None,
        )
    if dataset == "restricted_securities":
        return prefix + (
            row.get("security_id") or None, row.get("restriction_type") or None,
            parse_date(row.get("effective_from")), parse_date(row.get("effective_to")),
            row.get("status") or None,
        )
    raise ValueError(f"Unknown dataset {dataset}")


def load_reference_and_raw(
    cursor: Any, run_id: int, input_paths: dict[str, Path]
) -> dict[str, int]:
    """Load all five CSV files into Bronze and merge reference data."""
    counts: dict[str, int] = {}
    for dataset, (default_name, table) in INPUTS.items():
        path = input_paths.get(dataset, DATA_DIR / default_name)
        rows = read_csv(path, dataset)
        values = [
            raw_values(dataset, row, run_id, path.name, idx)
            for idx, row in enumerate(rows, start=1)
        ]
        if values:
            cursor.executemany(raw_insert_sql(table), values)
        counts[dataset] = len(values)

    # Merge source reference datasets into the curated reference tables.
    # This small prototype assumes one canonical row per key in each file.
    cursor.execute("""
        MERGE INTO SECURITY_MASTER T
        USING (
          SELECT SECURITY_ID, ISIN, ASSET_CLASS, CURRENCY,
                 EFFECTIVE_FROM, EFFECTIVE_TO
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM SECURITY_MASTER_RAW R
            WHERE RUN_ID = :run_id AND SECURITY_ID IS NOT NULL
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID)
        WHEN MATCHED THEN UPDATE SET
          T.ISIN = S.ISIN, T.ASSET_CLASS = S.ASSET_CLASS,
          T.CURRENCY = S.CURRENCY, T.EFFECTIVE_FROM = S.EFFECTIVE_FROM,
          T.EFFECTIVE_TO = S.EFFECTIVE_TO
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, ISIN, ASSET_CLASS, CURRENCY, EFFECTIVE_FROM, EFFECTIVE_TO)
          VALUES
          (S.SECURITY_ID, S.ISIN, S.ASSET_CLASS, S.CURRENCY,
           S.EFFECTIVE_FROM, S.EFFECTIVE_TO)
    """, {"run_id": run_id})

    cursor.execute("""
        MERGE INTO POSITIONS T
        USING (
          SELECT PORTFOLIO_ID, SECURITY_ID, POSITION_DATE,
                 QUANTITY, BOOK_VALUE, CURRENCY
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY PORTFOLIO_ID, SECURITY_ID, POSITION_DATE
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM POSITIONS_RAW R WHERE RUN_ID = :run_id
          ) WHERE RN = 1
        ) S
        ON (T.PORTFOLIO_ID = S.PORTFOLIO_ID
            AND T.SECURITY_ID = S.SECURITY_ID
            AND T.POSITION_DATE = S.POSITION_DATE)
        WHEN MATCHED THEN UPDATE SET
          T.QUANTITY = S.QUANTITY, T.BOOK_VALUE = S.BOOK_VALUE,
          T.CURRENCY = S.CURRENCY
        WHEN NOT MATCHED THEN INSERT
          (PORTFOLIO_ID, SECURITY_ID, POSITION_DATE, QUANTITY, BOOK_VALUE, CURRENCY)
          VALUES
          (S.PORTFOLIO_ID, S.SECURITY_ID, S.POSITION_DATE,
           S.QUANTITY, S.BOOK_VALUE, S.CURRENCY)
    """, {"run_id": run_id})

    cursor.execute("""
        MERGE INTO MARKET_PRICES T
        USING (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID, PRICE_DATE, PRICE_SOURCE
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM MARKET_PRICES_RAW R WHERE RUN_ID = :run_id
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID
            AND T.PRICE_DATE = S.PRICE_DATE
            AND T.PRICE_SOURCE = S.PRICE_SOURCE)
        WHEN MATCHED THEN UPDATE SET
          T.PRICE = S.PRICE, T.PRICE_CURRENCY = S.PRICE_CURRENCY
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE)
          VALUES
          (S.SECURITY_ID, S.PRICE_DATE, S.PRICE, S.PRICE_CURRENCY, S.PRICE_SOURCE)
    """, {"run_id": run_id})

    cursor.execute("""
        MERGE INTO RESTRICTED_SECURITIES T
        USING (
          SELECT SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM,
                 EFFECTIVE_TO, STATUS
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM RESTRICTED_SECURITIES_RAW R WHERE RUN_ID = :run_id
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID
            AND T.RESTRICTION_TYPE = S.RESTRICTION_TYPE
            AND T.EFFECTIVE_FROM = S.EFFECTIVE_FROM)
        WHEN MATCHED THEN UPDATE SET
          T.EFFECTIVE_TO = S.EFFECTIVE_TO, T.STATUS = S.STATUS
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO, STATUS)
          VALUES
          (S.SECURITY_ID, S.RESTRICTION_TYPE, S.EFFECTIVE_FROM,
           S.EFFECTIVE_TO, S.STATUS)
    """, {"run_id": run_id})
    return counts


def process_transactions(
    cursor: Any, run_id: int, business_date: date
) -> tuple[int, int, int, int]:
    """Validate raw transaction rows and merge accepted canonical records."""
    cursor.execute(
        """
        SELECT SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, PORTFOLIO_ID,
               SECURITY_ID, TRADE_DATE, QUANTITY, PRICE, CURRENCY
        FROM TRANSACTIONS_RAW
        WHERE RUN_ID = :run_id
        ORDER BY SOURCE_FILE, SOURCE_ROW_NUMBER
        """,
        {"run_id": run_id},
    )
    raw_rows = cursor.fetchall()
    received = len(raw_rows)

    valid_by_id: dict[str, tuple[Any, ...]] = {}
    rejects: list[tuple[Any, ...]] = []
    duplicates = 0

    for source_file, row_num, tx_id, portfolio, security, trade_date, qty, price, currency in raw_rows:
        reason = None
        if not tx_id:
            reason = "MISSING_TRANSACTION_ID"
        elif not portfolio:
            reason = "MISSING_PORTFOLIO_ID"
        elif not security:
            reason = "MISSING_SECURITY_ID"
        elif trade_date is None:
            reason = "MISSING_TRADE_DATE"
        elif qty is None or qty == 0:
            reason = "INVALID_QUANTITY"
        elif price is None or price <= 0:
            reason = "INVALID_PRICE"
        elif not currency:
            reason = "MISSING_CURRENCY"

        trade_day = trade_date.date() if isinstance(trade_date, datetime) else trade_date
        if reason is None and trade_day > business_date:
            reason = "FUTURE_TRADE_DATE"

        if reason is None:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM SECURITY_MASTER
                WHERE SECURITY_ID = :security
                  AND :trade_date >= EFFECTIVE_FROM
                  AND (EFFECTIVE_TO IS NULL OR :trade_date <= EFFECTIVE_TO)
                """,
                {"security": security, "trade_date": trade_day},
            )
            if cursor.fetchone()[0] == 0:
                reason = "UNKNOWN_OR_INACTIVE_SECURITY"

        if reason:
            rejects.append((run_id, source_file, row_num, tx_id, reason, None))
            continue

        normalized = (
            tx_id, portfolio, security, trade_day, qty, price,
            currency, source_file, row_num
        )
        prior = valid_by_id.get(tx_id)
        if prior is not None:
            duplicates += 1
            if prior[:7] != normalized[:7]:
                rejects.append((
                    run_id, source_file, row_num, tx_id,
                    "CONFLICTING_DUPLICATE_TRANSACTION",
                    "Same transaction ID has different business fields",
                ))
            # Identical duplicate: keep first canonical copy only.
            continue
        valid_by_id[tx_id] = normalized

    if rejects:
        cursor.executemany(
            """
            INSERT INTO TRANSACTION_REJECTS
              (RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, REASON, DETAILS)
            VALUES (:1, :2, :3, :4, :5, :6)
            """,
            rejects,
        )

    # Insert accepted records if not already present. If an existing ID has
    # changed business fields, do not overwrite it silently.
    accepted_new = 0
    for row in valid_by_id.values():
        tx_id, portfolio, security, trade_day, qty, price, currency, source_file, row_num = row
        record_hash = hashlib.sha256(
            "|".join(map(str, (tx_id, portfolio, security, trade_day, qty, price, currency))).encode()
        ).hexdigest()
        cursor.execute(
            "SELECT RECORD_HASH FROM TRANSACTIONS WHERE TRANSACTION_ID = :tx_id",
            {"tx_id": tx_id},
        )
        existing = cursor.fetchone()
        if existing:
            duplicates += 1
            if existing[0] != record_hash:
                rejects.append((
                    run_id, source_file, row_num, tx_id,
                    "CONFLICTING_DUPLICATE_TRANSACTION",
                    "Transaction ID already exists with different business fields",
                ))
                cursor.execute(
                    """
                    INSERT INTO TRANSACTION_REJECTS
                      (RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, REASON, DETAILS)
                    VALUES (:1, :2, :3, :4, :5, :6)
                    """,
                    rejects[-1],
                )
            continue
        cursor.execute(
            """
            INSERT INTO TRANSACTIONS (
              TRANSACTION_ID, PORTFOLIO_ID, SECURITY_ID, TRADE_DATE,
              QUANTITY, PRICE, CURRENCY, SOURCE_FILE, SOURCE_ROW_NUMBER,
              RUN_ID, RECORD_HASH
            ) VALUES (
              :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11
            )
            """,
            (
                tx_id, portfolio, security, trade_day, qty, price,
                currency, source_file, row_num, run_id, record_hash
            ),
        )
        accepted_new += 1

    return received, accepted_new, len(rejects), duplicates


def build_gold(cursor: Any, run_id: int, business_date: date) -> None:
    """Build run-scoped Gold outputs and explicit valuation exceptions."""
    # Clean only this run's output to support safe replay of the Gold step.
    for table in (
        "GOLD_COMPLIANCE_EXCEPTIONS",
        "GOLD_DATA_QUALITY_EXCEPTIONS",
        "GOLD_PORTFOLIO_POSITIONS",
    ):
        cursor.execute(f"DELETE FROM {table} WHERE RUN_ID = :run_id", {"run_id": run_id})

    cursor.execute(
        """
        INSERT INTO GOLD_DATA_QUALITY_EXCEPTIONS
          (RUN_ID, BUSINESS_DATE, PORTFOLIO_ID, SECURITY_ID, EXCEPTION_TYPE,
           DETAILS, SOURCE_POSITION_DATE)
        WITH SNAPSHOTS AS (
          SELECT P.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY PORTFOLIO_ID, SECURITY_ID
                   ORDER BY POSITION_DATE DESC
                 ) RN
          FROM POSITIONS P
          WHERE POSITION_DATE <= :business_date
        ),
        BALANCES AS (
          SELECT P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                 P.CURRENCY,
                 P.QUANTITY + NVL(SUM(T.QUANTITY), 0) AS CLOSING_QUANTITY
          FROM SNAPSHOTS P
          LEFT JOIN TRANSACTIONS T
            ON T.PORTFOLIO_ID = P.PORTFOLIO_ID
           AND T.SECURITY_ID = P.SECURITY_ID
           AND T.TRADE_DATE > P.POSITION_DATE
           AND T.TRADE_DATE <= :business_date
          WHERE P.RN = 1
          GROUP BY P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                   P.CURRENCY, P.QUANTITY
        ),
        LATEST_PRICE AS (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY
          FROM (
            SELECT M.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID
                     ORDER BY PRICE_DATE DESC
                   ) RN
            FROM MARKET_PRICES M
            WHERE PRICE_DATE <= :business_date
          )
          WHERE RN = 1
        )
        SELECT :run_id, :business_date, B.PORTFOLIO_ID, B.SECURITY_ID,
               CASE
                 WHEN M.SECURITY_ID IS NULL THEN 'MISSING_MARKET_PRICE'
                 WHEN M.PRICE <= 0 THEN 'INVALID_MARKET_PRICE'
                 WHEN M.PRICE_DATE < :business_date - 3 THEN 'STALE_MARKET_PRICE'
                 WHEN M.PRICE_CURRENCY <> B.CURRENCY THEN 'PRICE_CURRENCY_MISMATCH'
                 ELSE 'VALUATION_WITHHELD'
               END,
               'No eligible price for valuation; row not published',
               B.POSITION_DATE
        FROM BALANCES B
        LEFT JOIN LATEST_PRICE M ON M.SECURITY_ID = B.SECURITY_ID
        WHERE M.SECURITY_ID IS NULL
           OR M.PRICE IS NULL
           OR M.PRICE <= 0
           OR M.PRICE_DATE < :business_date - 3
           OR M.PRICE_CURRENCY <> B.CURRENCY
        """,
        {
            "run_id": run_id,
            "business_date": business_date,
        },
    )

    cursor.execute(
        """
        INSERT INTO GOLD_PORTFOLIO_POSITIONS (
          RUN_ID, BUSINESS_DATE, PORTFOLIO_ID, SECURITY_ID, QUANTITY,
          MARKET_PRICE, MARKET_VALUE, PRICE_DATE, PRICE_SOURCE, CURRENCY,
          SOURCE_POSITION_DATE
        )
        WITH SNAPSHOTS AS (
          SELECT P.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY PORTFOLIO_ID, SECURITY_ID
                   ORDER BY POSITION_DATE DESC
                 ) RN
          FROM POSITIONS P
          WHERE POSITION_DATE <= :business_date
        ),
        BALANCES AS (
          SELECT P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                 P.CURRENCY,
                 P.QUANTITY + NVL(SUM(T.QUANTITY), 0) AS CLOSING_QUANTITY
          FROM SNAPSHOTS P
          LEFT JOIN TRANSACTIONS T
            ON T.PORTFOLIO_ID = P.PORTFOLIO_ID
           AND T.SECURITY_ID = P.SECURITY_ID
           AND T.TRADE_DATE > P.POSITION_DATE
           AND T.TRADE_DATE <= :business_date
          WHERE P.RN = 1
          GROUP BY P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                   P.CURRENCY, P.QUANTITY
        ),
        LATEST_PRICE AS (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE
          FROM (
            SELECT M.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID
                     ORDER BY PRICE_DATE DESC
                   ) RN
            FROM MARKET_PRICES M
            WHERE PRICE_DATE <= :business_date
          )
          WHERE RN = 1
        )
        SELECT :run_id, :business_date, B.PORTFOLIO_ID, B.SECURITY_ID,
               B.CLOSING_QUANTITY, M.PRICE,
               ROUND(B.CLOSING_QUANTITY * M.PRICE, 2),
               M.PRICE_DATE, M.PRICE_SOURCE, B.CURRENCY, B.POSITION_DATE
        FROM BALANCES B
        JOIN LATEST_PRICE M ON M.SECURITY_ID = B.SECURITY_ID
        JOIN SECURITY_MASTER S ON S.SECURITY_ID = B.SECURITY_ID
        WHERE M.PRICE IS NOT NULL
          AND M.PRICE > 0
          AND M.PRICE_DATE >= :business_date - 3
          AND M.PRICE_CURRENCY = B.CURRENCY
          AND :business_date >= S.EFFECTIVE_FROM
          AND (S.EFFECTIVE_TO IS NULL OR :business_date <= S.EFFECTIVE_TO)
        """,
        {
            "run_id": run_id,
            "business_date": business_date,
        },
    )

    cursor.execute(
        """
        INSERT INTO GOLD_COMPLIANCE_EXCEPTIONS (
          RUN_ID, TRANSACTION_ID, PORTFOLIO_ID, SECURITY_ID, TRADE_DATE,
          RESTRICTION_TYPE, EXCEPTION_REASON, SOURCE_FILE, SOURCE_ROW_NUMBER
        )
        SELECT :run_id, T.TRANSACTION_ID, T.PORTFOLIO_ID, T.SECURITY_ID,
               T.TRADE_DATE, R.RESTRICTION_TYPE,
               'TRADE DURING ACTIVE RESTRICTION',
               T.SOURCE_FILE, T.SOURCE_ROW_NUMBER
        FROM TRANSACTIONS T
        JOIN RESTRICTED_SECURITIES R
          ON R.SECURITY_ID = T.SECURITY_ID
         AND T.TRADE_DATE >= R.EFFECTIVE_FROM
         AND (R.EFFECTIVE_TO IS NULL OR T.TRADE_DATE <= R.EFFECTIVE_TO)
        WHERE UPPER(R.STATUS) = 'ACTIVE'
          AND T.TRADE_DATE <= :business_date
        """,
        {"run_id": run_id, "business_date": business_date},
    )


def run_pipeline(
    business_date: date,
    fail_after_load: bool = False,
    force_replay: bool = False,
    input_paths: dict[str, Path] | None = None,
) -> int:
    input_paths = input_paths or {}
    connection = connect()
    started = time.monotonic()
    cursor = connection.cursor()
    run_id: int | None = None
    delivery_hashes: list[tuple[str, str]] = []

    try:
        run_id = insert_run(cursor, business_date)
        connection.commit()  # preserve run record if a later step fails

        # Register all input files before loading. Same set of content hashes
        # means the exact same five-file delivery.
        for dataset, (default_name, _) in INPUTS.items():
            path = input_paths.get(dataset, DATA_DIR / default_name)
            delivery_hashes.append((file_sha256(path), path.name))

        combined_hash = hashlib.sha256(
            "|".join(sorted(item[0] for item in delivery_hashes)).encode()
        ).hexdigest()

        if not force_replay:
            should_process = register_delivery(
                cursor, combined_hash, "five-file-delivery", run_id
            )
            if not should_process:
                cursor.execute(
                    """
                    UPDATE PIPELINE_RUNS
                    SET STATUS = 'SKIPPED_DUPLICATE',
                        COMPLETED_AT = SYSTIMESTAMP,
                        ELAPSED_SECONDS = :elapsed
                    WHERE RUN_ID = :run_id
                    """,
                    {"elapsed": time.monotonic() - started, "run_id": run_id},
                )
                connection.commit()
                print(f"Identical successful delivery already processed; run {run_id} skipped.")
                return run_id
            connection.commit()

        counts = load_reference_and_raw(cursor, run_id, input_paths)
        total_received = sum(counts.values())
        cursor.execute(
            """
            UPDATE PIPELINE_RUNS
            SET RECEIVED_COUNT = :received, STATUS = 'LOADED'
            WHERE RUN_ID = :run_id
            """,
            {"received": total_received, "run_id": run_id},
        )
        connection.commit()

        if fail_after_load:
            raise RuntimeError("Injected failure after raw load (requested test)")

        received, accepted, rejected, duplicates = process_transactions(
            cursor, run_id, business_date
        )
        build_gold(cursor, run_id, business_date)

        cursor.execute(
            """
            UPDATE PIPELINE_RUNS
            SET STATUS = 'SUCCEEDED',
                COMPLETED_AT = SYSTIMESTAMP,
                ACCEPTED_COUNT = :accepted,
                REJECTED_COUNT = :rejected,
                DUPLICATE_COUNT = :duplicates,
                ELAPSED_SECONDS = :elapsed
            WHERE RUN_ID = :run_id
            """,
            {
                "accepted": accepted,
                "rejected": rejected,
                "duplicates": duplicates,
                "elapsed": time.monotonic() - started,
                "run_id": run_id,
            },
        )
        cursor.execute(
            """
            UPDATE FILE_DELIVERIES
            SET STATUS = 'SUCCEEDED', COMPLETED_AT = SYSTIMESTAMP
            WHERE RUN_ID = :run_id
            """,
            {"run_id": run_id},
        )
        connection.commit()
        print(
            f"Run {run_id} succeeded. Input rows={total_received}; "
            f"transactions received={received}; accepted={accepted}; "
            f"rejected={rejected}; duplicates={duplicates}"
        )
        return run_id

    except Exception as exc:
        connection.rollback()
        if run_id is not None:
            cursor.execute(
                """
                UPDATE PIPELINE_RUNS
                SET STATUS = 'FAILED', ERROR_MESSAGE = :message,
                    COMPLETED_AT = SYSTIMESTAMP,
                    ELAPSED_SECONDS = :elapsed
                WHERE RUN_ID = :run_id
                """,
                {
                    "message": str(exc)[:1000],
                    "elapsed": time.monotonic() - started,
                    "run_id": run_id,
                },
            )
            cursor.execute(
                """
                UPDATE FILE_DELIVERIES
                SET STATUS = 'FAILED'
                WHERE RUN_ID = :run_id
                """,
                {"run_id": run_id},
            )
            connection.commit()
        print(f"Pipeline failed for run {run_id}: {exc}", file=sys.stderr)
        raise
    finally:
        cursor.close()
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--fail-after-load", action="store_true")
    parser.add_argument("--force-replay", action="store_true")
    args = parser.parse_args()
    business_date = datetime.strptime(args.business_date, "%Y-%m-%d").date()
    run_pipeline(
        business_date=business_date,
        fail_after_load=args.fail_after_load,
        force_replay=args.force_replay,
    )


if __name__ == "__main__":
    main()
