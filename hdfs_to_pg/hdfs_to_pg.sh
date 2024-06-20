#!/bin/bash
hdfs_exec=/opt/hadoop/bin/hdfs
set -e
# Default Value
HAS_HEADER="false"
DELIMITER=','
TRUNCATE_BEFORE_LOAD="false"
NO_PASSWORD="false"
# Function to display usage
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -p, --hdfs-path <path>    HDFS path to CSV files (required)"
    echo "  -H, --host <host>         PostgreSQL host (required)"
    echo "  -D, --database <database> PostgreSQL database (required)"
    echo "  -S, --schema <schema>     PostgreSQL schema (required)"
    echo "  -T, --table <table>       PostgreSQL table (required)"
    echo "  -U, --user <user>         PostgreSQL user (required)"
    echo "  --delimiter <char>        Delimiter used in CSV files (default: \"$DELIMITER\")"
    echo "  --truncate                Truncate table before loading"
    echo "  --header                  Has header in CSV files"
    echo "  --no-password             Never issue a postgres password prompt. If the server requires password authentication and a password is not available by other means such as a .pgpass file, the connection attempt will fail. This option can be useful in batch jobs and scripts where no user is present to enter a password."
    exit 1
}

# Parse options
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -h | --help)
            usage;;
        -p|--hdfs-path) 
            HDFS_PATH="$2"; 
            shift ;;
        -H|--host) 
            HOST="$2"; 
            shift ;;
        -D|--database) 
            DATABASE="$2"; 
            shift ;;
        -S|--schema) 
            SCHEMA="$2"; 
            shift ;;
        -T|--table) 
            TABLE="$2"; 
            shift ;;
        -U|--pg-user) 
            USER="$2"; 
            shift ;;
        --truncate) 
            TRUNCATE_BEFORE_LOAD="true"
            shift ;;
        --header) 
            HAS_HEADER="true"; 
            shift ;;
        --delimiter) 
            DELIMITER="$2"; 
            shift ;;
        --no-password)
            NO_PASSWORD="true"
            ;;
        *)
            echo "Error: Unexpected argument $1"
            usage
            ;;
    esac
    shift
done

# Check if required options are provided
if [ -z "$HDFS_PATH" ]; then
    echo "Error: HDFS path (-p or --hdfs-path) is required."
    usage
fi
if [ -z "$HOST" ]; then
    echo "Error: PostgreSQL host (-H or --host) is required."
    usage
fi
if [ -z "$DATABASE" ]; then
    echo "Hello"
    echo "Error: PostgreSQL database (-D or --database) is required."
    usage
fi
if [ -z "$SCHEMA" ]; then
    echo "Error: PostgreSQL schema (-S or --schema) is required."
    usage
fi
if [ -z "$TABLE" ]; then
    echo "Error: PostgreSQL table (-T or --table) is required."
    usage
fi
if [ -z "$USER" ]; then
    echo "Error: PostgreSQL user (-U or --pg-user) is required."
    usage
fi


# Ask for PGPASSWORD
if [ "$NO_PASSWORD" = "false" ]; then
    read -s -p "Enter Postgres Password for User <$USER>: " PASSWORD
    export PGPASSWORD=$PASSWORD
fi


# Get file list from HDFS
files=$($hdfs_exec dfs -ls $HDFS_PATH | grep '\.csv$' | awk '{print $8}')
num_files=$(echo "$files" | wc -l)
echo -e "Found $num_files csv part files on HDFS: \n$files\n"

# Truncate Table
if [ "$TRUNCATE_BEFORE_LOAD" = "true" ];then 
    psql -h $HOST -U $USER -d $DATABASE -c "TRUNCATE TABLE $SCHEMA.$TABLE;"
    echo -e "TABLE TRUNCATED. \n"
fi

# Loop through all files and load to PG
i=1
for file in $files; do
    echo "Loading file ($i/$num_files): $file"
    $hdfs_exec dfs -cat $file | psql -h $HOST -U $USER -d $DATABASE --no-password -c "\copy $SCHEMA.$TABLE FROM STDIN WITH (FORMAT csv, HEADER $HAS_HEADER, DELIMITER \"$DELIMITER\")"
    let i=i+1
done 

echo "All csv Files Copied Successfully!"