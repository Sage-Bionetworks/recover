/*
  Copy Parquet data from its stage location into its respective table for all data types.

  This is meant to be used from the CLI as a bootstrap script and is not designed
  to be used as part of the data-loading process for a production pipeline.
  Please refer to the stored procedure `copy_into_table_from_{datatype}_parquet_stage`
  invoked by this script for the specifics of the `COPY INTO` command.

  Command line variables:
  database_name - The name of the database where our tables and stage exist.
  schema_name - The name of the schema where our tables and stage exist.
 */

USE ROLE RECOVER_DATA_ENGINEER;
USE WAREHOUSE RECOVER_XSMALL;
USE DATABASE &{ database_name };
USE SCHEMA &{ schema_name };
WITH copy_into_each_parquet_table AS PROCEDURE ()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
DECLARE
  parquet_datatypes ARRAY := [
    'enrolledparticipants_customfields_symptoms',
    'enrolledparticipants_customfields_treatments',
    'enrolledparticipants',
    'fitbitactivitylogs',
    'fitbitdailydata',
    'fitbitdevices',
    'fitbitecg',
    'fitbitecg_waveformsamples',
    'fitbitintradaycombined',
    'fitbitrestingheartrates',
    'fitbitsleeplogs',
    'fitbitsleeplogs_sleeplogdetails',
    'googlefitsamples',
    'healthkitv2activitysummaries',
    'healthkitv2electrocardiogram',
    'healthkitv2electrocardiogram_subsamples',
    'healthkitv2heartbeat',
    'healthkitv2heartbeat_subsamples',
    'healthkitv2samples',
    'healthkitv2statistics',
    'healthkitv2workouts_events',
    'healthkitv2workouts',
    'symptomlog',
    'symptomlog_value_symptoms',
    'symptomlog_value_treatments'
  ];
  datatype VARCHAR;
  procedure_name VARCHAR;
BEGIN
  FOR i in 0 to array_size(:parquet_datatypes)-1 DO
    datatype := GET(:parquet_datatypes, :i);
    procedure_name := 'copy_into_table_from_' || :datatype || '_parquet_stage';
    CALL IDENTIFIER(:procedure_name)(
      target_table => :datatype
    );
  END FOR;
END;
$$
CALL copy_into_each_parquet_table();
