import time
import pymysql
import psycopg2

from DBSync.auto_synchronize import auto_synchronize
from DBSync.data_differences import data_differences
from DBSync.data_fetchers import fetch_mysql_data, fetch_postgres_data, fetch_data, fetch_db_data
from DBSync.data_synchronization import synchronize_tables
from DBSync.utilities import load_yaml


def main():
    mappings_config = load_yaml('config/mappings.yaml')
    source_config = load_yaml('config/source.yaml')
    target_config = load_yaml('config/target.yaml')
    fk_mappings = mappings_config.get('foreign_keys', [])

    if not mappings_config:
        print("Error loading mappings configuration. Exiting.")
        return

    for mapping in mappings_config['mappings']:

        reference_table_name = mapping['reference_table']
        target_table_name = mapping['target_table']
        key_column_mapping = mapping['key_column_mapping']

        reference_table_info = next((table for table in source_config['tables']
                                     if table['name'] == reference_table_name), None)

        if reference_table_info is None:
            print(f"No source configuration found for {reference_table_name}.")
            continue

        print(reference_table_info)

        target_table_info = next((table for table in target_config['tables']
                                  if table['name'] == target_table_name), None)

        if target_table_info is None:
            print(f"No target configuration found for {target_table_name}.")
            continue

        print(target_table_info)

        reference_db_type = reference_table_info['db_type']
        target_db_type = target_table_info['db_type']
        print(reference_db_type, "-", target_db_type)

        if reference_db_type == "MySQL":
            reference_conn = pymysql.connect(host='localhost', user='springstudent', password='springstudent',
                                             database='customer_database')
        if reference_db_type == "PostgreSQL":
            reference_conn = psycopg2.connect(host='localhost', user='postgres', password='root',
                                              database='customer_database')

        if target_db_type == "MySQL":
            target_conn = pymysql.connect(host='localhost', user='springstudent', password='springstudent',
                                          database='customer_database')
        if target_db_type == "PostgreSQL":
            target_conn = psycopg2.connect(host='localhost', user='postgres', password='root',
                                           database='customer_database')

        if not reference_conn:
            print("Connection to Reference Database failed.")
            return

        print("Connected to Reference Database successfully.")

        if not target_conn:
            print("Connection to Target Database failed.")
            return
        print("Connected to Target Database successfully.\n")

        start_time = time.time()
        print(f"Processing mapping for Reference Table '{reference_table_name}' and Target Table '{target_table_name}'...")

        reference_data = fetch_db_data(reference_conn, reference_table_info['name'], reference_table_info['columns'], reference_db_type)
        target_data = fetch_db_data(target_conn, target_table_info['name'], target_table_info['columns'], target_db_type)

        insert_commands, update_commands, insert_reference_commands = synchronize_tables(
            reference_data, target_data, reference_table_info['key_column'], target_table_info['key_column'],
            reference_table_name, target_table_name, key_column_mapping, reference_conn, target_conn, fk_mappings, reference_table_info, target_table_info)

        data_differences(reference_data, target_data, reference_table_info['key_column'], target_table_info['key_column'], key_column_mapping)

        end_time = time.time()
        print(f"Total execution time taken: {end_time - start_time} seconds.")

        # Writing SQL commands to files
        file_name = f"results/mapping_sql_commands_{reference_table_name}_to_{target_table_name}.sql"
        with open(file_name, 'w') as file:
            file.write("\n".join(insert_commands + update_commands))

        file_name1 = f"results/mapping_sql_commands_{target_table_name}_to_{reference_table_name}.sql"
        with open(file_name1, 'w') as file1:
            file1.write("\n".join(insert_reference_commands))

        print(f"\nSQL commands have been written to '{file_name}'.")
        print(f"\nReverse SQL commands have been written to '{file_name1}'.")

        auto_sync_choice = input("\nDo you want to auto synchronize the target databases? (yes/no): ").lower()
        if auto_sync_choice == "yes":
            auto_synchronize(file_name, target_conn)
            reference_data = fetch_db_data(reference_conn, reference_table_info['name'], reference_table_info['columns'], reference_db_type)

        auto_sync_choice = input("\nDo you want to auto synchronize the reference databases? (yes/no): ").lower()
        if auto_sync_choice == "yes":
            auto_synchronize(file_name1, reference_conn)
            reference_data = fetch_db_data(reference_conn, reference_table_info['name'], reference_table_info['columns'], reference_db_type)

        # Closing database connections
        reference_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()