import csv
import psycopg2
import os

csv_file = 'load_csv_to_pg/test/input.csv'
schema_name = 'jhuacg'
table_name = os.path.basename(csv_file).split('.')[0]

# Function to read the CSV file and analyze the header
def analyze_csv_header(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
    return header

# Function to generate DDL based on CSV header
def generate_ddl(table_name, csv_file):
    header = analyze_csv_header(csv_file)
    ddl_columns = ', '.join([col + ' VARCHAR(255)' for col in header])
    ddl = f'CREATE TABLE {table_name} ({ddl_columns});'
    return ddl


# with psycopg2.connect(**db_params) as conn:
#     cursor = conn.cursor()
#     cursor.execute(generate_ddl(f'{schema_name}.{table_name}', header))
#     conn.commit()
