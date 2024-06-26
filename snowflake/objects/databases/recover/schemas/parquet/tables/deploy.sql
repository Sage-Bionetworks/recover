/*
  CREATE all tables and COPY INTO data from S3
*/

/*
  We are unable to use the CREATE OR ALTER syntax with the
  USING TEMPLATE clause. Since we want to infer the schema
  and load data from scratch in this simplified bootstrap
  scenario, it's appropriate to replace any existing tables.
*/
CREATE OR REPLACE TABLE garminsleepsummary
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        WITHIN GROUP(ORDER BY order_id)
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@{{ parquet_stage_name }}/dataset_garminsleepsummary',
                FILE_FORMAT => '{{ parquet_file_format_name }}'
            )
        )
    );
/*
COPY INTO enrolledparticipants
    FROM @{{ parquet_stage_name }}/dataset_enrolledparticipants
    FILE_FORMAT = (
        FORMAT_NAME = '{{ parquet_file_format_name }}'
    )
    MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
*/
