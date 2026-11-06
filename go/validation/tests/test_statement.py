# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
import uuid

import adbc_drivers_validation.tests.statement as statement_tests
import pyarrow

from . import bigquery, utils

RESULT_BUFFER_SIZE = "adbc.bigquery.sql.query.result_buffer_size"


def pytest_generate_tests(metafunc) -> None:
    quirks = [bigquery.get_quirks(metafunc.config.getoption("vendor_version"))]
    return statement_tests.generate_tests(quirks, metafunc)


def run_without_hanging(fn, timeout: float = 60) -> None:
    errors = []

    def target() -> None:
        try:
            fn()
        except Exception as e:
            errors.append(e)

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(timeout=timeout)
    assert not worker.is_alive(), f"driver did not return within {timeout}s"
    assert not errors, errors


def execute_bound_query(cursor, query: str, parameters: pyarrow.RecordBatch):
    statement = cursor.adbc_statement
    statement.set_options(**{RESULT_BUFFER_SIZE: 1})
    statement.set_sql_query(query)
    statement.bind(parameters)
    handle, _ = statement.execute_query()
    return pyarrow.RecordBatchReader._import_from_c(handle.address)


def test_bound_parameters_with_results_larger_than_buffer(driver, conn) -> None:
    values = []

    def execute_and_read() -> None:
        with conn.cursor() as cursor:
            reader = execute_bound_query(
                cursor, "SELECT ? AS x", pyarrow.record_batch({"p": [1, 2, 3]})
            )
            values.extend(reader.read_all()["x"].to_pylist())

    run_without_hanging(execute_and_read)
    assert sorted(values) == [1, 2, 3]


def test_bound_parameters_result_dropped_unread(driver, conn) -> None:
    def execute_and_drop() -> None:
        with conn.cursor() as cursor:
            reader = execute_bound_query(
                cursor, "SELECT ? AS x", pyarrow.record_batch({"p": [1, 2, 3]})
            )
            reader.close()

    run_without_hanging(execute_and_drop)


def test_result_dropped_after_partial_read(driver, conn) -> None:
    def execute_and_drop() -> None:
        with conn.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_options(**{RESULT_BUFFER_SIZE: 1})
            statement.set_sql_query(
                "SELECT x FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS x"
            )
            handle, _ = statement.execute_query()
            reader = pyarrow.RecordBatchReader._import_from_c(handle.address)
            reader.read_next_batch()
            reader.close()

    run_without_hanging(execute_and_drop)


class TestStatement(statement_tests.TestStatement):
    @utils.retry_rate_limit
    def test_rows_affected(self, driver, conn) -> None:
        super().test_rows_affected(driver, conn)


def test_dry_run(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{"adbc.bigquery.sql.query.dry_run": True})
        cursor.execute("SELECT 1 AS a, 'foobar' as b")
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "a"
        assert cursor.description[1][0] == "b"

        cursor.execute("SELECT 1 AS a, 'foobar' as b", parameters=[(1,), (2,)])
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "a"
        assert cursor.description[1][0] == "b"

        cursor.execute("SELECT 1 AS a, 'foobar' as b", parameters=[(1,), (2,)])
        schema = cursor.fetchallarrow().schema
        assert schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SELECT"


def test_script_no_results(driver, conn) -> None:
    # Regression test for https://github.com/dbt-labs/dbt-core/issues/16081
    # That bug never made it into the Driver Foundry driver, but guard against
    # it regardless.
    target_table = f"test_script_no_results_{uuid.uuid4().hex}"

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {target_table} (idx INT, val INT)")
            cursor.execute(f"""
            CREATE TEMP TABLE staging AS SELECT 1 AS idx, 2 AS val;
            MERGE INTO {target_table} AS DEST
            USING (SELECT * FROM staging) AS SOURCE
            ON DEST.idx = SOURCE.idx
            WHEN MATCHED THEN
                UPDATE SET val = SOURCE.val
            WHEN NOT MATCHED THEN
                INSERT (idx, val)
                VALUES (SOURCE.idx, SOURCE.val);
            DROP TABLE IF EXISTS staging;
            """)
            assert not cursor.description
            schema = cursor.fetch_arrow_table().schema
            assert (
                schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SCRIPT"
            )

            cursor.execute(f"SELECT val FROM {target_table} WHERE idx = 1")
            assert cursor.fetchone() == (2,)
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=target_table)


def test_script_results(driver, conn) -> None:
    # This should read the first result set
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1; SELECT 'foobar'")
        table = cursor.fetch_arrow_table()
        schema = table.schema
        assert schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SCRIPT"

        assert len(table) == 1, repr(table)


STORAGE_API_DISABLED = "bigquery.query.use_storage_api_disabled_client"


def test_disable_storage_api_option_roundtrip(driver, conn) -> None:
    with conn.cursor() as cursor:
        statement = cursor.adbc_statement
        assert statement.get_option(STORAGE_API_DISABLED) == "false"
        statement.set_options(**{STORAGE_API_DISABLED: "true"})
        assert statement.get_option(STORAGE_API_DISABLED) == "true"


def test_disable_storage_api_pseudo_columns(driver, conn) -> None:
    # Ingestion-time partitioning is what exposes _PARTITIONDATE and
    # _PARTITIONTIME. Routing through the row-based reader must return their
    # values rather than nulls.
    table = "validation_disable_storage_api_pseudo_columns"
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE OR REPLACE TABLE {table} (a INT64) PARTITION BY _PARTITIONDATE"
        )
        cursor.execute(f"INSERT INTO {table} (a) VALUES (7)")

    try:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_options(**{STORAGE_API_DISABLED: "true"})
            cursor.execute(
                f"SELECT a, _PARTITIONDATE AS pd, _PARTITIONTIME AS pt FROM {table}"
            )
            rows = cursor.fetch_arrow_table().to_pylist()

        assert len(rows) == 1
        # INT64 columns need an integer builder on the row-based path.
        assert rows[0]["a"] == 7
        assert rows[0]["pd"] is not None
        assert rows[0]["pt"] is not None
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table)


def test_disable_storage_api_pagination(driver, conn) -> None:
    # The row-based iterator paginates every 1000 rows. Ensure iterating over
    # all batches returns the entire result set
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{STORAGE_API_DISABLED: "true"})
        cursor.execute("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 2000)) AS x")
        table = cursor.fetch_arrow_table()
        assert len(table) == 2000
        assert table["x"][0].as_py() == 1
        assert table["x"][1999].as_py() == 2000
