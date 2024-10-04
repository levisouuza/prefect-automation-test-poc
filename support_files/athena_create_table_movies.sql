CREATE EXTERNAL TABLE IF NOT EXISTS database_sor.movies_table (
    movieid STRING,
    movietitle STRING,
    movieyear STRING,
    movieurl STRING,
    movierank STRING,
    critic_score STRING,
    audience_score STRING
)
PARTITIONED BY (anomesdia int)
STORED AS PARQUET
LOCATION 's3://development-test-levis-sor/movies_table/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');