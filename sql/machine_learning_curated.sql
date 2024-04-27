CREATE EXTERNAL TABLE `machine_learning_curated`(
  `distancefromobject` int COMMENT 'from deserializer',
  `x` float COMMENT 'from deserializer',
  `y` float COMMENT 'from deserializer',
  `z` float COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'dots.in.keys'='FALSE',
  'ignore.malformed.json'='FALSE',
  'mapping'='TRUE')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dpsinghvij-bucket/ml-curated'
TBLPROPERTIES (
  'classification'='json',
  'transient_lastDdlTime'='1714179523')