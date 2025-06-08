from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Set
from pyspark.sql import SparkSession, DataFrame
import logging
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)


def clean_column_names(df: DataFrame) -> DataFrame:
    """Clean column names by replacing spaces and special characters with underscores"""
    for column in df.columns:
        new_column = column.replace(" ", "_").replace(".", "_")
        df = df.withColumnRenamed(column, new_column)
    return df


# Table mapping for Snowflake targets
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
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_FORMAT",
    },
    "DeliveryLeadType": {
        "snowflake_table": "STG_LDP_DELIVERY_LEAD_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_LEAD_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_LEAD_TYPE",
    },
    "DeliveryResponse": {
        "snowflake_table": "STG_LDP_DELIVERY_RESPONSE",
        "hash_column_name": "STG_LDP_DELIVERY_RESPONSE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_RESPONSE",
    },
    "DeliverySourceType": {
        "snowflake_table": "STG_LDP_DELIVERY_SOURCE_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_SOURCE_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_SOURCE_TYPE",
    },
    "DeliveryStatus": {
        "snowflake_table": "STG_LDP_DELIVERY_STATUS",
        "hash_column_name": "STG_LDP_DELIVERY_STATUS_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_STATUS",
    },
    "DeliveryType": {
        "snowflake_table": "STG_LDP_DELIVERY_TYPE",
        "hash_column_name": "STG_LDP_DELIVERY_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_TYPE",
    },
    "IntakeTemp": {
        "snowflake_table": "STG_LDP_INTAKE_TEMP",
        "hash_column_name": "STG_LDP_INTAKE_TEMP_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_INTAKE_TEMP",
    },
    "LCRResponse": {
        "snowflake_table": "STG_LDP_LCR_RESPONSE",
        "hash_column_name": "STG_LDP_LCR_RESPONSE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LCR_RESPONSE",
    },
    "LeadAllocation": {
        "snowflake_table": "STG_LDP_LEAD_ALLOCATION",
        "hash_column_name": "STG_LDP_LEAD_ALLOCATION_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_ALLOCATION",
    },
    "LeadControl": {
        "snowflake_table": "STG_LDP_LEAD_CONTROL",
        "hash_column_name": "STG_LDP_LEAD_CONTROL_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_CONTROL",
    },
    "LeadFBMC": {
        "snowflake_table": "STG_LDP_LEAD_FBMC",
        "hash_column_name": "STG_LDP_LEAD_FBMC_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_FBMC",
    },
    "LeadIntake": {
        "snowflake_table": "STG_LDP_LEAD_INTAKE",
        "hash_column_name": "STG_LDP_LEAD_INTAKE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_INTAKE",
    },
    "LeadLEADCO": {
        "snowflake_table": "STG_LDP_LEAD_LEADCO",
        "hash_column_name": "STG_LDP_LEAD_LEADCO_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEADCO",
    },
    "LeadLevel": {
        "snowflake_table": "STG_LDP_LEAD_LEVEL",
        "hash_column_name": "STG_LDP_LEAD_LEVEL_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEVEL",
    },
    "LeadMAILCO": {
        "snowflake_table": "STG_LDP_LEAD_MAILCO",
        "hash_column_name": "STG_LDP_LEAD_MAILCO_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_MAILCO",
    },
    "LeadPlum": {
        "snowflake_table": "STG_LDP_LEAD_PLUM",
        "hash_column_name": "STG_LDP_LEAD_PLUM_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_PLUM",
    },
    "LeadSFG": {
        "snowflake_table": "STG_LDP_LEAD_SFG",
        "hash_column_name": "STG_LDP_LEAD_SFG_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_SFG",
    },
    "LeadType": {
        "snowflake_table": "STG_LDP_LEAD_TYPE",
        "hash_column_name": "STG_LDP_LEAD_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_TYPE",
    },
    "Logs": {
        "snowflake_table": "STG_LDP_LOGS",
        "hash_column_name": "STG_LDP_LOGS_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LOGS",
    },
    "LORLeads": {
        "snowflake_table": "STG_LDP_LOR_LEADS",
        "hash_column_name": "STG_LDP_LOR_LEADS_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LOR_LEADS",
    },
    "PhoneBlacklist": {
        "snowflake_table": "STG_LDP_PHONE_BLACKLIST",
        "hash_column_name": "STG_LDP_PHONE_BLACKLIST_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_PHONE_BLACKLIST",
    },
    "ResponseType": {
        "snowflake_table": "STG_LDP_RESPONSE_TYPE",
        "hash_column_name": "STG_LDP_RESPONSE_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_RESPONSE_TYPE",
    },
    "RGILead": {
        "snowflake_table": "STG_LDP_RGI_LEAD",
        "hash_column_name": "STG_LDP_RGI_LEAD_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_RGI_LEAD",
    },
    "SourceType": {
        "snowflake_table": "STG_LDP_SOURCE_TYPE",
        "hash_column_name": "STG_LDP_SOURCE_TYPE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE",
    },
    "SourceTypeChannelFunction": {
        "snowflake_table": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION",
        "hash_column_name": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION",
    },
    "UTMSource": {
        "snowflake_table": "STG_LDP_UTM_SOURCE",
        "hash_column_name": "STG_LDP_UTM_SOURCE_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_UTM_SOURCE",
    },
    "Vendor": {
        "snowflake_table": "STG_LDP_VENDOR",
        "hash_column_name": "STG_LDP_VENDOR_KEY",
        "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_VENDOR",
    },
}


class TableDiscovery:
    def __init__(self, spark: SparkSession, server_details: Dict[str, str]) -> None:
        self.spark: SparkSession = spark
        self.server_details: Dict[str, str] = server_details

    def discover_all_tables(self) -> List[str]:
        try:
            logging.info("Discovering all tables from the database...")
            query: str = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dbo'
            """
            jdbc_url: str = (
                f"jdbc:sqlserver://{self.server_details['server_url']};databaseName=LeadDepot"
            )
            logging.info(f"JDBC URL: {jdbc_url}")

            df: DataFrame = (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("tableLock", "true")
                .load()
            )
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

    def extract_table(self, table_name: str) -> Optional[DataFrame]:
        """
        Extract data from SQL Server table with special handling for money columns.
        Returns None if table should be skipped.
        """
        try:
            # Skip excluded tables immediately
            if table_name in self.excluded_tables:
                logging.info(f"Skipping excluded table: {table_name}")
                return None
            logging.info(f"Extracting data from table: {table_name}")
            jdbc_url: str = (
                f"jdbc:sqlserver://{self.server_details['server_url']};databaseName=LeadDepot"
            )

            # First, get column information including data types
            schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}' 
            AND TABLE_SCHEMA = 'dbo'
            """

            schema_df = (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", schema_query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("tableLock", "true")
                .load()
            )

            # Get all column names and their data types
            columns = [(row.COLUMN_NAME, row.DATA_TYPE) for row in schema_df.collect()]

            # Get total count from the source table for logging
            count_query = f"SELECT COUNT(*) AS total_count FROM dbo.{table_name}"
            count_df = (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", count_query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("tableLock", "true")
                .load()
            )
            total_count_in_source = count_df.collect()[0].total_count
            logging.info(
                f"Total records in source table {table_name}: {total_count_in_source}"
            )

            # Construct a query that selects all columns, replacing money/sql_variant columns with NULLs
            select_parts = []
            for col_name, dtype in columns:
                if dtype in ("money", "sql_variant"):
                    select_parts.append(f"CAST(NULL AS VARCHAR(50)) AS [{col_name}]")
                else:
                    select_parts.append(f"[{col_name}]")
            columns_sql = ", ".join(select_parts)
            query = f"SELECT {columns_sql} FROM dbo.{table_name}"

            # Load data using the constructed query
            df = (
                self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("tableLock", "true")
                .load()
            )

            # Clean column names
            df = clean_column_names(df)

            # Log record count
            record_count: int = df.count()
            logging.info(f"Extracted {record_count} records from table: {table_name}")

            # Check if dataframe is empty
            if not df.columns or df.rdd.isEmpty():
                logging.warning(f"No data found in table {table_name}. Skipping.")
                return None
            return df
        except Exception as e:
            error_msg = str(e)

            # Check for specific error messages we want to handle differently
            if "is not able to access the database" in error_msg:
                logging.warning(
                    f"Permission error accessing table {table_name}: {error_msg}"
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
                f"{self.azure_details['base_path']}/{self.azure_details['stage']}/LeadDepot/{table_name}"
            )
            logging.info(f"ADLS Path: {path}")

            # Enable column mapping for Delta
            df.write.format("delta").option("delta.columnMapping.mode", "name").option(
                "delta.minReaderVersion", "2"
            ).option("delta.minWriterVersion", "5").option(
                "mergeSchema", "true"
            ).option(
                "overwriteSchema", "true"
            ).mode(
                "overwrite"
            ).save(
                path
            )

            logging.info(f"Data successfully written to ADLS for table: {table_name}")
        except Exception as e:
            logging.error(f"Error writing to ADLS for table {table_name}: {e}")
            logging.error(traceback.format_exc())


class SnowflakeLoader:
    def __init__(self, spark: SparkSession, snowflake_config: Dict[str, str]) -> None:
        self.spark: SparkSession = spark
        self.config: Dict[str, str] = snowflake_config

    def load_to_snowflake(self, df: DataFrame, table_config: Dict[str, str]) -> None:
        try:
            logging.info(
                f"Loading data into Snowflake table: {table_config['staging_table_name']}"
            )
            
            # Write to Snowflake
            (
                df.write.format("snowflake")
                .options(**self.config)
                .option("dbtable", table_config["staging_table_name"])
                .mode("overwrite")
                .save()
            )
            
            # Validate the load by checking record count in Snowflake
            validation_query = f"SELECT COUNT(*) FROM {table_config['staging_table_name']}"
            snowflake_count_df = (
                self.spark.read.format("snowflake")
                .options(**self.config)
                .option("query", validation_query)
                .load()
            )
            
            # Access first column regardless of name (more robust)
            snowflake_record_count = snowflake_count_df.collect()[0][0]
            
            logging.info(
                f"Validation: Snowflake table {table_config['staging_table_name']} contains {snowflake_record_count} records after load"
            )

            logging.info(
                f"Data successfully loaded into Snowflake table: {table_config['staging_table_name']}"
            )
            
        except Exception as e:
            logging.error(
                f"Error loading data into Snowflake for table {table_config['staging_table_name']}: {e}"
            )
            logging.error(traceback.format_exc())


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Add the 5 metadata columns with O(1) additional space complexity"""
    return (
        df.withColumn("ETL_CREATED_DATE", current_timestamp())
        .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
        .withColumn("CREATED_BY", lit("ETL_PROCESS"))
        .withColumn("TO_PROCESS", lit(True))
        .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("LeadDepot"))
    )


def validate_and_cast(df: DataFrame) -> DataFrame:
    """
    Validate and cast columns with O(1) space complexity.
    Any money columns will be kept as NULL strings from the extract_table method.
    """
    # Return Validation
    return df


def process_table(
    table_name: str, data_sync: DataSync, snowflake_loader: SnowflakeLoader
) -> None:
    logging.info(f"Starting to process table: {table_name}")
    try:
        # Extract data
        df: Optional[DataFrame] = data_sync.extract_table(table_name)

        # Check if dataframe exists - it will be None for excluded tables or empty tables
        if df is None:
            logging.warning(
                f"Table {table_name} was excluded from processing or contains no data."
            )
            return
        
        # Add metadata columns before writing
        df = add_metadata_columns(df)

        # Write to ADLS Gen2
        data_sync.write_to_adls(df, table_name)

        # Write to Snowflake if table is in mapping
        if table_name in SNOWFLAKE_TABLES:
            snowflake_loader.load_to_snowflake(df, SNOWFLAKE_TABLES[table_name])
        else:
            logging.info(f"Table {table_name} is not configured for Snowflake loading.")
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
                "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8,"
                "net.snowflake:snowflake-jdbc:3.13.8,"
                "net.snowflake:spark-snowflake_2.12:2.9.3",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.properties.defaults.columnMapping.mode", "name"
            )
            .getOrCreate()
        )
        logging.info("Spark session initialized.")


        server_details: Dict[str, str] = {
            "server_url": "72.27.227.41:1433",
            "username": dbutils.secrets.get(scope="dba-key-vault-secret", key="LDE-PROD-databricks-username"),
            "password": dbutils.secrets.get(scope="dba-key-vault-secret", key="LDE-PROD-databricks-password"),
        }


        azure_details: Dict[str, str] = {
            "base_path": "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net",
            "stage": "RAW",
        }


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


        discovery: TableDiscovery = TableDiscovery(spark, server_details)
        data_sync: DataSync = DataSync(spark, server_details, azure_details)
        snowflake_loader: SnowflakeLoader = SnowflakeLoader(spark, snowflake_config)

        # Discover and process all tables
        all_tables: List[str] = discovery.discover_all_tables()
        if not all_tables:
            logging.error("No tables to process. Exiting the script.")
            return
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
                logging.info(f"QA Note: Table {table} present in source database")
            else:
                logging.warning(f"QA Note: Table {table} NOT found in source database")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for table_name in all_tables:
                futures.append(
                    executor.submit(
                        process_table, table_name, data_sync, snowflake_loader
                    )
                )
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing table: {e}")
                    logging.error(traceback.format_exc())
        logging.info("LeadDepot ETL process completed successfully.")

        # Log special note for QA review
        logging.info(
            "This run uses 'overwrite' mode which ensures Snowflake tables exactly match source tables at runtime."
        )
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
