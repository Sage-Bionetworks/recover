/*
  A stored procedure which copies data from a named stage into a table.

  Because of limitations in how we can pass variables to stage names,
  this procedure is specific to a stage location.  That is, we cannot
  use Snowflake scripting variables within the stage name, so we instead
  use Jinja variables, which has the side effect of "fixing" the procedure
  to use a specific stage location.

  Jinja templating variables:
  stage_name - The name of the stage where we copy data from
  stage_path - The location within the stage where our data is
  file_format - The name of the file format object used during copy
 */
CREATE OR REPLACE PROCEDURE copy_into_table_from_stage(
    target_table VARCHAR
)
  RETURNS TABLE ()
  LANGUAGE SQL
as
$$
DECLARE
  res RESULTSET DEFAULT (
    COPY INTO IDENTIFIER(:target_table)
      FROM @{{ stage_name }}/{{ stage_path }}
      FILE_FORMAT = (
          FORMAT_NAME = '{{ file_format }}'
      )
      MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  );
BEGIN
  RETURN TABLE(res);
END;
$$;
