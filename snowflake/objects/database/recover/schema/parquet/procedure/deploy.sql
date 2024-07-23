/*
  Deploy all PROCEDURE objects

  Jinja templating variables:
  stage_name - The name of the stage where our data exists.
  file_format - The name of the file format object used by the
    `copy_into_table_from_stage.sql` procedure.
*/

WITH create_procedure_for_each_parquet_table AS PROCEDURE ()
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
  dataset_name VARCHAR;
BEGIN
  FOR i in 0 to array_size(:parquet_datatypes)-1 DO
    datatype := GET(:parquet_datatypes, :i);
    dataset_name := CONCAT('dataset_', :datatype);
    -- Create a stored procedure which uses this data type's stage location
    EXECUTE IMMEDIATE
      FROM './copy_into_table_from_stage.sql'
      USING (
        datatype => :datatype,
        stage_name => '{{ stage_name }}',
        stage_path => :dataset_name,
        file_format => '{{ file_format }}'
      );
  END FOR;
END;
$$
CALL create_procedure_for_each_parquet_table();
