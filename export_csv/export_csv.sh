#!/bin/bash

# PostgreSQL settings
# PGUSER=
# PGPASSWORD=
# PGDATABASE=
# PGHOST=

SCHEMA="genpop_1k_by_site"

# Directory to save CSV files
EXPORT_DIR="result"

# Tables to export
declare -a TABLES=("prescribing"  "private_demographic"  "lab_history"  "lab_result_cm"  "procedures"  "version_history"  "vital"  "condition"  "death"  "encounter"  "enrollment"  "harvest"  "hash_token"  "immunization"  "lds_address_history"  "pcornet_trial"  "person_visit_start2001"  "private_address_geocode"  "private_address_history"  "pro_cm"  "death_cause"  "demographic"  "diagnosis"  "dispensing"  "med_admin"  "obs_clin"  "provider"  "obs_gen")

# Export each table
for TABLE in "${TABLES[@]}"
do
    echo "Exporting $SCHEMA.$TABLE..."
    PGPASSWORD=$PGPASSWORD psql -U $PGUSER -h $PGHOST -d $PGDATABASE -c "\COPY (SELECT * FROM $SCHEMA.$TABLE) TO '$EXPORT_DIR/$TABLE.csv' CSV HEADER"
done

echo "Export completed."