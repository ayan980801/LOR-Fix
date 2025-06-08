from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
import logging
import traceback
import re
import sys
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    sha2,
    concat_ws,
    col,
)
from delta.tables import DeltaTable
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    RetryError,
)

# -------------------------------------------------
# Choose ETL mode: "historical" or "incremental"
# -------------------------------------------------
ETL_MODE = "incremental"  # or "historical"

"""
ETL_MODE = "historical"
    - Overwrites all data in ADLS and Snowflake for every table.
ETL_MODE = "incremental"
    - Inserts/updates new and changed records using the hash column.
    - Deletes target records that are not present in the current source snapshot.
    - Requires a hash column to be defined in SNOWFLAKE_TABLES mapping.
    - Only use if you are reading the FULL source table each run.
    - If no hash column, falls back to overwrite mode.
"""

# ---------------------
# Handle dbutils import
# ---------------------
try:
    dbutils  # type: ignore
except NameError:
    import IPython
    dbutils = IPython.get_ipython().user_ns.get("dbutils", None)
    if dbutils is None:
        raise ImportError(
            "dbutils is not available in this environment. "
            "This script is intended for Databricks."
        )

# --------------------
# Configure logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)


def _log_retry(retry_state) -> None:
    """Log retry attempts without exposing sensitive details."""
    exc = retry_state.outcome.exception()
    attempt = retry_state.attempt_number
    func_name = retry_state.fn.__name__ if hasattr(retry_state, "fn") else "operation"
    logging.warning(
        f"Retry {attempt} for {func_name} due to {type(exc).__name__}" if exc else f"Retry {attempt} for {func_name}"
    )

# -------------------------------
# Robust column name cleaning
# -------------------------------


def clean_column_names(df: DataFrame) -> DataFrame:
    """Clean column names by replacing any non-alphanumeric character
    with underscores, check for collisions."""
    original_cols = df.columns
    cleaned_cols = [re.sub(r'\W+', '_', c) for c in original_cols]
    if len(set(cleaned_cols)) != len(cleaned_cols):
        raise ValueError(
            f"Column name collision detected after cleaning: {cleaned_cols}")
    return df.toDF(*cleaned_cols)


# ---------------------------------
# Table mapping for Snowflake targets
# ---------------------------------
SNOWFLAKE_TABLES: Dict[str, Dict[str, str]] = {
    "County": {
        "snowflake_table": "STG_LDP_COUNTY",
        "hash_column_name": "STG_LDP_COUNTY_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_COUNTY",
    },
    "DataMAILCO": {
        "snowflake_table": "STG_LDP_DATAMAILCO",
        "hash_column_name": "STG_LDP_DATAMAILCO_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DATAMAILCO",
    },
    "Delivery": {
        "snowflake_table": "STG_LDP_DELIVERY",
        "hash_column_name": "STG_LDP_DELIVERY_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY",
    },
    "DeliveryArea": {
        "snowflake_table": "STG_LDP_DELIVERY_AREA",
        "hash_column_name": "STG_LDP_DELIVERY_AREA_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_AREA",
    },
    "DeliveryFormat": {
        "snowflake_table": "STG_LDP_DELIVERY_FORMAT",
        "hash_column_name": "STG_LDP_DELIVERY_FORMAT_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_FORMAT"
        ),
    },
    "DeliveryLeadType": {
        "snowflake_table": "STG_LDP_DELIVERY_LEAD_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_LEAD_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_LEAD_TYPE"
        ),
    },
    "DeliveryResponse": {
        "snowflake_table": "STG_LDP_DELIVERY_RESPONSE",
        "hash_column_name": "STG_LDP_DELIVERY_RESPONSE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_RESPONSE"
        ),
    },
    "DeliverySourceType": {
        "snowflake_table": "STG_LDP_DELIVERY_SOURCE_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_SOURCE_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_SOURCE_TYPE"
        ),
    },
    "DeliveryStatus": {
        "snowflake_table": "STG_LDP_DELIVERY_STATUS",
        "hash_column_name": "STG_LDP_DELIVERY_STATUS_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_STATUS"
        ),
    },
    "DeliveryType": {
        "snowflake_table": "STG_LDP_DELIVERY_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_TYPE"
        ),
    },
    "IntakeTemp": {
        "snowflake_table": "STG_LDP_INTAKE_TEMP",
        "hash_column_name": "STG_LDP_INTAKE_TEMP_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_INTAKE_TEMP"
        ),
    },
    "LCRResponse": {
        "snowflake_table": "STG_LDP_LCR_RESPONSE",
        "hash_column_name": "STG_LDP_LCR_RESPONSE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LCR_RESPONSE"
        ),
    },
    "LeadAllocation": {
        "snowflake_table": "STG_LDP_LEAD_ALLOCATION",
        "hash_column_name": "STG_LDP_LEAD_ALLOCATION_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_ALLOCATION"
        ),
    },
    "LeadControl": {
        "snowflake_table": "STG_LDP_LEAD_CONTROL",
        "hash_column_name": "STG_LDP_LEAD_CONTROL_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_CONTROL"
        ),
    },
    "LeadFBMC": {
        "snowflake_table": "STG_LDP_LEAD_FBMC",
        "hash_column_name": "STG_LDP_LEAD_FBMC_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_FBMC"
        ),
    },
    "LeadIntake": {
        "snowflake_table": "STG_LDP_LEAD_INTAKE",
        "hash_column_name": "STG_LDP_LEAD_INTAKE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_INTAKE"
        ),
    },
    "LeadLEADCO": {
        "snowflake_table": "STG_LDP_LEAD_LEADCO",
        "hash_column_name": "STG_LDP_LEAD_LEADCO_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEADCO"
        ),
    },
    "LeadLevel": {
        "snowflake_table": "STG_LDP_LEAD_LEVEL",
        "hash_column_name": "STG_LDP_LEAD_LEVEL_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEVEL"
        ),
    },
    "LeadMAILCO": {
        "snowflake_table": "STG_LDP_LEAD_MAILCO",
        "hash_column_name": "STG_LDP_LEAD_MAILCO_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_MAILCO"
        ),
    },
    "LeadPlum": {
        "snowflake_table": "STG_LDP_LEAD_PLUM",
        "hash_column_name": "STG_LDP_LEAD_PLUM_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_PLUM"
        ),
    },
    "LeadSFG": {
        "snowflake_table": "STG_LDP_LEAD_SFG",
        "hash_column_name": "STG_LDP_LEAD_SFG_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_SFG"
        ),
    },
    "LeadType": {
        "snowflake_table": "STG_LDP_LEAD_TYPE",
        "hash_column_name": "STG_LDP_LEAD_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_TYPE"
        ),
    },
    "Logs": {
        "snowflake_table": "STG_LDP_LOGS",
        "hash_column_name": "STG_LDP_LOGS_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LOGS"
        ),
    },
    "LORLeads": {
        "snowflake_table": "STG_LDP_LOR_LEADS",
        "hash_column_name": "STG_LDP_LOR_LEADS_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_LOR_LEADS"
        ),
    },
    "PhoneBlacklist": {
        "snowflake_table": "STG_LDP_PHONE_BLACKLIST",
        "hash_column_name": "STG_LDP_PHONE_BLACKLIST_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_PHONE_BLACKLIST"
        ),
    },
    "ResponseType": {
        "snowflake_table": "STG_LDP_RESPONSE_TYPE",
        "hash_column_name": "STG_LDP_RESPONSE_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_RESPONSE_TYPE"
        ),
    },
    "RGILead": {
        "snowflake_table": "STG_LDP_RGI_LEAD",
        "hash_column_name": "STG_LDP_RGI_LEAD_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_RGI_LEAD"
        ),
    },
    "SourceType": {
        "snowflake_table": "STG_LDP_SOURCE_TYPE",
        "hash_column_name": "STG_LDP_SOURCE_TYPE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE"
        ),
    },
    "SourceTypeChannelFunction": {
        "snowflake_table": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION",
        "hash_column_name": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION"
        ),
    },
    "UTMSource": {
        "snowflake_table": "STG_LDP_UTM_SOURCE",
        "hash_column_name": "STG_LDP_UTM_SOURCE_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_UTM_SOURCE"
        ),
    },
    "Vendor": {
        "snowflake_table": "STG_LDP_VENDOR",
        "hash_column_name": "STG_LDP_VENDOR_KEY",
        "staging_table_name": (
            "DEV.QUILITY_EDW_STAGE.STG_LDP_VENDOR"
        ),
    },
}


def add_hash_column(df: DataFrame, table_name: str) -> DataFrame:
    """Add a hash column for the table if configured."""
    hash_column = SNOWFLAKE_TABLES.get(table_name, {}).get("hash_column_name")
    if not hash_column:
        logging.info(
            f"No hash column configured for table {table_name}. Skipping hash generation."
        )
        return df

    exclude_cols = {
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
    }

    columns_to_hash = [c for c in df.columns if c not in exclude_cols and c != hash_column]
    if not columns_to_hash:
        logging.warning(f"No columns available for hashing in table {table_name}.")
        df = df.withColumn(hash_column, sha2(lit(""), 256))
    else:
        df = df.withColumn(
            hash_column,
            sha2(concat_ws("||", *[col(c).cast("string") for c in columns_to_hash]), 256),
        )

    new_col_order = [hash_column] + [c for c in df.columns if c != hash_column]
    df = df.select(new_col_order)

    sample_values = [r[hash_column] for r in df.select(hash_column).limit(5).collect()]
    logging.info(
        f"Hash column {hash_column} created for table {table_name}. Samples: {sample_values}"
    )
    return df


class TableDiscovery:
    def __init__(
        self, spark: SparkSession, server_details: Dict[str, str]
    ) -> None:
        self.spark: SparkSession = spark
        self.server_details: Dict[str, str] = server_details

    def _read_sql_server_with_retry(self, jdbc_url: str, query: str) -> DataFrame:
        """Read from SQL Server with retries."""

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=_log_retry,
            reraise=True,
        )
        def _read() -> DataFrame:
            return (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("tableLock", "true")
                .load()
            )

        try:
            return _read()
        except RetryError as exc:
            logging.error("SQL Server read failed after retries")
            raise exc.last_attempt.exception()

    def discover_all_tables(self) -> List[str]:
        try:
            logging.info("Discovering all tables from the database...")
            query: str = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE LOWER(TABLE_SCHEMA) = 'dbo'
            """
            jdbc_url: str = (
                f"jdbc:sqlserver://{self.server_details['server_url']};"
                "databaseName=LeadDepot;"
                "encrypt=true;"
                "trustServerCertificate=true;"
            )
            logging.info(f"JDBC URL: {jdbc_url}")

            df: DataFrame = self._read_sql_server_with_retry(jdbc_url, query)
            tables: List[str] = [row.TABLE_NAME for row in df.collect()]
            logging.info(f"Discovered tables: {tables}")
            if not tables:
                logging.warning("No tables found in the schema 'dbo'.")
            return tables
        except Exception as e:
            logging.error(f"Error discovering tables: {e}")
            logging.error(traceback.format_exc())
            return []


class DataSync:
    def __init__(
        self,
        spark: SparkSession,
        server_details: Dict[str, str],
        azure_details: Dict[str, str],
    ) -> None:
        self.spark: SparkSession = spark
        self.server_details: Dict[str, str] = server_details
        self.azure_details: Dict[str, str] = azure_details
        # Tables to exclude from processing due to permission or other issues
        self.excluded_tables: set = {
            "MI_LeadIntakeAnalysis",
            "MI_LeadIntakeMortgageAmounts",
        }

    def _read_sql_server_with_retry(self, jdbc_url: str, query: str) -> DataFrame:
        """Read from SQL Server with retries."""

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=_log_retry,
            reraise=True,
        )
        def _read() -> DataFrame:
            return (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("tableLock", "true")
                .load()
            )

        try:
            return _read()
        except RetryError as exc:
            logging.error("SQL Server read failed after retries")
            raise exc.last_attempt.exception()

    def extract_table(
        self, table_name: str
    ) -> Optional[DataFrame]:
        """
        Extract data from SQL Server table with special handling for money
        columns. Returns None if table should be skipped.
        """
        try:
            # Skip excluded tables immediately
            if table_name in self.excluded_tables:
                logging.info(f"Skipping excluded table: {table_name}")
                return None
            logging.info(f"Extracting data from table: {table_name}")
            jdbc_url: str = (
                f"jdbc:sqlserver://{self.server_details['server_url']};"
                "databaseName=LeadDepot;"
                "encrypt=true;"
                "trustServerCertificate=true;"
            )

            # First, get column information including data types
            schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND LOWER(TABLE_SCHEMA) = 'dbo'
            """

            schema_df = self._read_sql_server_with_retry(jdbc_url, schema_query)

            columns = [
                (row.COLUMN_NAME, row.DATA_TYPE)
                for row in schema_df.collect()
            ]

            # Log columns being replaced with NULL due to unsupported types
            money_cols = [
                col
                for col, dtype in columns
                if dtype in ("money", "sql_variant")
            ]
            if money_cols:
                logging.warning(
                    f"Columns {money_cols} in table {table_name} will be "
                    "replaced with NULLs due to unsupported data type."
                )

            # Get total count from the source table for logging
            count_query = (
                f"SELECT COUNT(*) AS total_count FROM dbo.{table_name}"
            )
            count_df = self._read_sql_server_with_retry(jdbc_url, count_query)
            total_count_in_source = count_df.collect()[0].total_count
            logging.info(
                f"Total records in source table {table_name}: "
                f"{total_count_in_source}"
            )

            # Construct a query that selects all columns, replacing
            # money/sql_variant columns with NULLs
            select_parts = []
            for col_name, dtype in columns:
                if dtype in ("money", "sql_variant"):
                    select_parts.append(
                        f"CAST(NULL AS VARCHAR(50)) AS [{col_name}]")
                else:
                    select_parts.append(f"[{col_name}]")
            columns_sql = ", ".join(select_parts)
            query = f"SELECT {columns_sql} FROM dbo.{table_name}"

            # Load data using the constructed query
            df = self._read_sql_server_with_retry(jdbc_url, query)

            # Clean column names
            df = clean_column_names(df)

            # Confirm data extracted without expensive count
            logging.info(
                f"Data extracted from table: {table_name}"
            )

            # Efficient empty dataframe check
            if not df.columns or df.rdd.isEmpty():
                logging.warning(
                    f"No data found in table {table_name}. Skipping."
                )
                return None
            return df
        except Exception as e:
            error_msg = str(e)

            # Check for specific error messages we want to handle differently
            if "is not able to access the database" in error_msg:
                logging.warning(
                    f"Permission error accessing table {table_name}: "
                    f"{error_msg}"
                )

                # Add this table to excluded tables for future runs
                self.excluded_tables.add(table_name)
            else:
                logging.error(f"Error extracting table {table_name}: {e}")
                logging.error(traceback.format_exc())

            # Return None to indicate this table should be skipped
            return None

    def write_to_adls(self, df: DataFrame, table_name: str) -> None:
        try:
            logging.info(f"Writing data to ADLS for table: {table_name}")
            path: str = (
                f"{self.azure_details['base_path']}/"
                f"{self.azure_details['stage']}/LeadDepot/{table_name}"
            )
            logging.info(f"ADLS Path: {path}")

            hash_column = SNOWFLAKE_TABLES.get(table_name, {}).get("hash_column_name")
            logging.info(f"ETL_MODE is {ETL_MODE}")

            if df.rdd.isEmpty():
                logging.warning(
                    f"Source DataFrame for table {table_name} is empty." 
                    "Full sync will delete all target records if they exist."
                )
                if ETL_MODE != "historical" and hash_column and DeltaTable.isDeltaTable(self.spark, path):
                    delta_table = DeltaTable.forPath(self.spark, path)
                    delta_table.delete("true")
                    logging.info(
                        f"All records deleted from ADLS Delta table for table: {table_name}"
                    )
                else:
                    logging.warning(
                        f"No delete performed for table {table_name} (table may not exist or ETL_MODE is historical)."
                    )
                return

            if ETL_MODE == "historical" or not hash_column:
                # Historical overwrite mode or no hash column available
                df.write.format("delta").option(
                    "delta.columnMapping.mode",
                    "name",
                ).option(
                    "delta.minReaderVersion",
                    "2",
                ).option(
                    "delta.minWriterVersion",
                    "5",
                ).option(
                    "mergeSchema",
                    "true",
                ).option(
                    "overwriteSchema",
                    "true",
                ).mode(
                    "overwrite",
                ).save(
                    path,
                )
                logging.info(
                    f"Data overwritten in ADLS for table: {table_name}"
                )
            else:
                # Incremental merge mode with full synchronization
                if DeltaTable.isDeltaTable(self.spark, path):
                    delta_table = DeltaTable.forPath(self.spark, path)
                    merge_condition = f"source.{hash_column} = target.{hash_column}"

                    delta_table.alias("target").merge(
                        source=df.alias("source"),
                        condition=merge_condition,
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

                    temp_view = f"source_hashes_{table_name}"
                    df.select(hash_column).distinct().createOrReplaceTempView(temp_view)
                    delete_condition = f"{hash_column} NOT IN (SELECT {hash_column} FROM {temp_view})"
                    deleted_count = delta_table.delete(delete_condition)
                    logging.info(
                        f"Delta merge and delete completed for table: {table_name}"
                    )
                else:
                    df.write.format("delta").option(
                        "delta.columnMapping.mode",
                        "name",
                    ).option(
                        "delta.minReaderVersion",
                        "2",
                    ).option(
                        "delta.minWriterVersion",
                        "5",
                    ).option(
                        "mergeSchema",
                        "true",
                    ).option(
                        "overwriteSchema",
                        "true",
                    ).mode(
                        "overwrite",
                    ).save(
                        path,
                    )
                    logging.info(
                        f"Initial Delta table created for table: {table_name}"
                    )
        except Exception as e:
            logging.error(f"Error writing to ADLS for table {table_name}: {e}")
            logging.error(traceback.format_exc())


class SnowflakeLoader:
    def __init__(
        self, spark: SparkSession, snowflake_config: Dict[str, str]
    ) -> None:
        self.spark: SparkSession = spark
        self.config: Dict[str, str] = snowflake_config

    def _write_snowflake_with_retry(
        self, df: DataFrame, table: str, mode: str = "overwrite"
    ) -> None:
        """Write DataFrame to Snowflake with retries."""

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=_log_retry,
            reraise=True,
        )
        def _write() -> None:
            (
                df.write.format("snowflake")
                .options(**self.config)
                .option("dbtable", table)
                .mode(mode)
                .save()
            )

        try:
            _write()
        except RetryError as exc:
            logging.error("Snowflake write failed after retries")
            raise exc.last_attempt.exception()

    def _read_snowflake_with_retry(self, query: str) -> DataFrame:
        """Read from Snowflake with retries."""

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=_log_retry,
            reraise=True,
        )
        def _read() -> DataFrame:
            return (
                self.spark.read.format("snowflake")
                .options(**self.config)
                .option("query", query)
                .load()
            )

        try:
            return _read()
        except RetryError as exc:
            logging.error("Snowflake read failed after retries")
            raise exc.last_attempt.exception()

    def load_to_snowflake(
        self, df: DataFrame, table_config: Dict[str, str]
    ) -> None:
        try:
            table = table_config["staging_table_name"]
            hash_column = table_config.get("hash_column_name")
            logging.info(f"ETL_MODE is {ETL_MODE}")

            if ETL_MODE == "historical" or not hash_column:
                logging.info(
                    f"Loading data into Snowflake table (overwrite): {table}"
                )
                self._write_snowflake_with_retry(df, table, mode="overwrite")
            else:
                temp_stage_table = table + "_STAGE"
                logging.info(
                    f"Loading data into temp Snowflake table: {temp_stage_table}"
                )
                self._write_snowflake_with_retry(df, temp_stage_table, mode="overwrite")

                set_clause = ", ".join([f"{c} = source.{c}" for c in df.columns])
                insert_cols = ", ".join(df.columns)
                insert_vals = ", ".join([f"source.{c}" for c in df.columns])
                merge_sql = f"""
                    MERGE INTO {table} AS target
                    USING {temp_stage_table} AS source
                    ON target.{hash_column} = source.{hash_column}
                    WHEN MATCHED THEN UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
                """
                logging.info("Executing Snowflake MERGE statement")
                self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
                    self.config, merge_sql
                )

                delete_sql = f"DELETE FROM {table} WHERE {hash_column} NOT IN (SELECT {hash_column} FROM {temp_stage_table})"
                logging.info("Executing Snowflake DELETE statement for full sync")
                self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
                    self.config, delete_sql
                )

                drop_sql = f"DROP TABLE IF EXISTS {temp_stage_table}"
                self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
                    self.config, drop_sql
                )

            validation_query = f"SELECT COUNT(*) FROM {table}"
            snowflake_count_df = self._read_snowflake_with_retry(validation_query)
            snowflake_record_count = snowflake_count_df.collect()[0][0]

            logging.info(
                f"Validation: Snowflake table {table} contains {snowflake_record_count} records after load"
            )
            logging.info(
                f"Data successfully loaded into Snowflake table: {table}"
            )

        except Exception as e:
            logging.error(
                f"Error loading data into Snowflake for table "
                f"{table_config['staging_table_name']}: {e}"
            )
            logging.error(traceback.format_exc())


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Add the 5 metadata columns, warn if columns already exist
    (will be overwritten)"""
    for col in [
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
    ]:
        if col in df.columns:
            logging.warning(
                f"Column {col} already exists and will be overwritten in "
                "add_metadata_columns()."
            )
    return (
        df.withColumn("ETL_CREATED_DATE", current_timestamp())
        .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
        .withColumn("CREATED_BY", lit("ETL_PROCESS"))
        .withColumn("TO_PROCESS", lit(True))
        .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("LeadDepot"))
    )


def process_table(
    table_name: str, data_sync: DataSync, snowflake_loader: SnowflakeLoader
) -> None:
    logging.info(f"Starting to process table: {table_name}")
    try:
        # Extract data
        df: Optional[DataFrame] = data_sync.extract_table(table_name)

        # Check if dataframe exists - it will be None for excluded tables
        # or empty tables
        if df is None:
            logging.warning(
                f"Table {table_name} was excluded from processing or "
                "contains no data."
            )
            return

        # Add hash column and metadata columns
        df = add_hash_column(df, table_name)
        df = add_metadata_columns(df)

        # Write to ADLS Gen2
        data_sync.write_to_adls(df, table_name)

        # Write to Snowflake if table is in mapping
        if table_name in SNOWFLAKE_TABLES:
            snowflake_loader.load_to_snowflake(
                df, SNOWFLAKE_TABLES[table_name])
        else:
            logging.info(
                f"Table {table_name} is not configured for Snowflake loading.")
    except Exception as e:
        logging.error(f"Error processing table {table_name}: {e}")
        logging.error(traceback.format_exc())


def main() -> None:
    try:
        logging.info("Starting the LeadDepot ETL process...")

        # Initialize Spark session with necessary configurations
        spark: SparkSession = (
            SparkSession.builder.appName("LeadDepotETL")
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8",
                        "net.snowflake:snowflake-jdbc:3.13.8",
                        "net.snowflake:spark-snowflake_2.12:2.9.3",
                    ]
                ),
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                (
                    "spark.databricks.delta.properties.defaults."
                    "columnMapping.mode"
                ),
                "name",
            )
            .getOrCreate()
        )
        logging.info("Spark session initialized.")

        try:
            server_details: Dict[str, str] = {
                "server_url": "72.27.227.41:1433",
                "username": dbutils.secrets.get(
                    scope="dba-key-vault-secret",
                    key="LDE-PROD-databricks-username",
                ),
                "password": dbutils.secrets.get(
                    scope="dba-key-vault-secret",
                    key="LDE-PROD-databricks-password",
                ),
            }
        except Exception as e:
            logging.error(f"Error fetching SQL Server secrets: {e}")
            sys.exit(1)

        azure_details: Dict[str, str] = {
            "base_path": (
                "abfss://dataarchitecture@quilitydatabricks."
                "dfs.core.windows.net"
            ),
            "stage": "RAW",
        }

        try:
            snowflake_config: Dict[str, str] = {
                "sfURL": "https://hmkovlx-nu26765.snowflakecomputing.com",
                "sfUser": dbutils.secrets.get(
                    scope="key-vault-secret", key="DataProduct-SF-EDW-User"
                ),
                "sfPassword": dbutils.secrets.get(
                    scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
                ),
                "sfDatabase": "DEV",
                "sfWarehouse": "INTEGRATION_COMPUTE_WH",
                "sfSchema": "QUILITY_EDW_STAGE",
                "sfRole": "SG-SNOWFLAKE-DEVELOPERS",
            }
        except Exception as e:
            logging.error(f"Error fetching Snowflake secrets: {e}")
            sys.exit(1)

        discovery: TableDiscovery = TableDiscovery(spark, server_details)
        data_sync: DataSync = DataSync(spark, server_details, azure_details)
        snowflake_loader: SnowflakeLoader = SnowflakeLoader(
            spark, snowflake_config)

        # Discover and process all tables
        all_tables: List[str] = discovery.discover_all_tables()
        if not all_tables:
            logging.error("No tables to process. Exiting the script.")
            sys.exit(1)
        logging.info(f"Total tables to process: {len(all_tables)}")

        # Log QA information tables to help with troubleshooting
        qa_tables = [
            "LeadSFG",
            "RGILead",
            "LeadMAILCO",
            "LeadFBMC",
            "LeadLEADCO",
            "LeadPlum",
        ]
        for table in qa_tables:
            if table in all_tables:
                logging.info(
                    f"QA Note: Table {table} present in source database")
            else:
                logging.warning(
                    f"QA Note: Table {table} NOT found in source database")
        for table_name in all_tables:
            try:
                process_table(table_name, data_sync, snowflake_loader)
            except Exception as e:
                logging.error(f"Error processing table {table_name}: {e}")
                logging.error(traceback.format_exc())
        logging.info("LeadDepot ETL process completed successfully.")

        # Log special note for QA review
        logging.info(f"ETL run completed in '{ETL_MODE}' mode.")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
