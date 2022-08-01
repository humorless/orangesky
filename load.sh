#!/usr/bin/env bash

PGOPTIONS="--search_path=dbt_orangesky"                                                                                                
export PGOPTIONS 

psql -d orangesky -c "DROP TABLE IF EXISTS portfolio;"   
psql -d orangesky -c "CREATE SCHEMA IF NOT EXISTS dbt_orangesky;"   
psql -d orangesky -c "CREATE TABLE orangesky.dbt_orangesky.portfolio (
\"order_id\" bigint,
\"name\" text,
\"side\" text,
\"price\" numeric,
\"cost\" numeric,
\"volume_executed\" numeric,
\"time_created\" bigint
);"

psql -d orangesky -c "\copy portfolio FROM './seeds/portfolio.csv' HEADER CSV;"
