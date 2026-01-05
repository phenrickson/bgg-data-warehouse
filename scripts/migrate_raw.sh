#!/bin/bash

# Migrate raw dataset tables from old project to new project

bq query --use_legacy_sql=false "INSERT INTO \`bgg-data-warehouse.raw.thing_ids\` SELECT * FROM \`gcp-demos-411520.bgg_raw_prod.thing_ids\`"
bq query --use_legacy_sql=false "INSERT INTO \`bgg-data-warehouse.raw.raw_responses\` SELECT * FROM \`gcp-demos-411520.bgg_raw_prod.raw_responses\`"
bq query --use_legacy_sql=false "INSERT INTO \`bgg-data-warehouse.raw.fetched_responses\` SELECT * FROM \`gcp-demos-411520.bgg_raw_prod.fetched_responses\`"
bq query --use_legacy_sql=false "INSERT INTO \`bgg-data-warehouse.raw.fetch_in_progress\` SELECT * FROM \`gcp-demos-411520.bgg_raw_prod.fetch_in_progress\`"
bq query --use_legacy_sql=false "INSERT INTO \`bgg-data-warehouse.raw.processed_responses\` SELECT * FROM \`gcp-demos-411520.bgg_raw_prod.processed_responses\`"
