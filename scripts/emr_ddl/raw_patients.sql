CREATE OR REPLACE EXTERNAL TABLE  `emr-data-pipeline.emr_analytics.raw_patients`
WITH PARTITION COLUMNS (
  ingested_at TIMESTAMP
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://emr-data-pipeline-emr_analytics/emr/staging/patients/*'],
  hive_partition_uri_prefix = 'gs://emr-data-pipeline-emr_analytics/emr/staging/patients/'
);


-- drop table `emr-data-pipeline.emr_analytics.raw_patients`