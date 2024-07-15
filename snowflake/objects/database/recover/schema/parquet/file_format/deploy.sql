/*
  Deploy all file formats
*/
EXECUTE IMMEDIATE
  FROM './parquet_format.sql'
  USING (
    parquet_file_format_name => '{{ parquet_file_format_name }}'
  );
