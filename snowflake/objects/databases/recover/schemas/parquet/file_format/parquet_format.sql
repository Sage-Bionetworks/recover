/*
  Create the Parquet file format
*/
CREATE OR REPLACE FILE FORMAT {{ parquet_file_format_name }}
    TYPE = PARQUET
    COMPRESSION = AUTO
    USE_VECTORIZED_SCANNER = TRUE;
