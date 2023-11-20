import csv
import psycopg2
import os
import click

# Function to read the CSV file and analyze the header
def _analyze_csv_header(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
    return header

# Function to generate DDL based on CSV header
def _generate_ddl(table_name, header):
    ddl_columns = ', '.join([col + ' VARCHAR(255)' for col in header])
    ddl = f'CREATE TABLE {table_name} ({ddl_columns});'
    return ddl

@click.command()
@click.option('--schema_name', required=True, help='Schema of destination table')
@click.option('--table_name', required=False, default='', help='Schema of destination table')
@click.argument('csv_file', type=click.Path(exists=True))
def generate_ddl(schema_name, table_name, csv_file):
    if not table_name:
        table_name = os.path.basename(csv_file).split('.')[0]
    header = _analyze_csv_header(csv_file)
    ddl = _generate_ddl(f'{schema_name}.{table_name}', header)
    click.echo(ddl)

if __name__ == '__main__':
    generate_ddl()