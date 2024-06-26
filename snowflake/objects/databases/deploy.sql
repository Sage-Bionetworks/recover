/*
  The current maximum allowed execution depth of EXECUTE IMMEDIATE FROM
  statements is 5. Unfortunately, that makes a call stack which looks like:

  depth
  0      deploy.sql =>
  1        databases/deploy.sql =>
  2          databases/recover/deploy.sql =>
  3            databases/recover/schemas/deploy.sql =>
  4              databases/recover/schemas/parquet/deploy.sql =>
  5                databases/recover/schemas/parquet/tables/deploy.sql =>
  6                  databases/recover/schemas/parquet/tables/enrolled_participants.sql

  not possible. To circumvent this issue, we omit the highest level of
  abstraction (databases/deploy.sql) and instead EXECUTE IMMEDIATE FROM
  database deployments individually in the primary deployment script (deploy.sql)
*/
