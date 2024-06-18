#!/bin/bash
set -e

# HDFS CSV File Config
HDFS_PATH=/harvellj_test/all_asthma_dx
HAS_HEADER=TRUE
# Postgres Config
PGHOST="pedspbd02.research.chop.edu"
PGDATABASE="postgres"
PGSCHEMA="test_hdfs"
PGTABLE="test"
PGUSER="shenq"
# Loading Behavior Config
TRUNCATE_BEFORE_LOAD="true" # whether to truncate table before copy


hdfs_exec=/opt/hadoop/bin/hdfs

# Ask for PGPASSWORD
read -s -p "PGPASSWORD: " PGPASSWORD
export PGPASSWORD=$PGPASSWORD
# Loop through each line in the file using cat
files=$($hdfs_exec dfs -ls $HDFS_PATH | grep '\.csv$' | awk '{print $8}')
echo "Found csv part files:
$files"
if [ "$TRUNCATE_BEFORE_LOAD" = "true" ];then 
    psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "TRUNCATE TABLE $PGSCHEMA.$PGTABLE;"
    echo "TBALE TRUNCATED. "
fi
for file in $files; do
    echo "Loading file: $file"
    $hdfs_exec dfs -cat $file | psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "\copy $PGSCHEMA.$PGTABLE FROM STDIN WITH (FORMAT csv, HEADER $HAS_HEADER)"
done && echo "All csv Files Copied Successfully!"