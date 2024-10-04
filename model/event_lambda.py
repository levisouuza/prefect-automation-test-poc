EVENT_LAMBDA = {
    "provider": "imdb",
    "business": "movies",
    "source_bucket": "bucket_out_account_external",
    "destination_bucket": "development-test-levis-stage",
    "glue_jobname": "jobstagetosoringestion",
    "worker_type": "G.1X",
    "numbers_worker": "2",
    "parameter_sor": "/sor/imdb/movies"
}

