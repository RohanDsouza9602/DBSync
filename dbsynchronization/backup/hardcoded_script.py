import time
import pymysql
import psycopg2

from DBSync.auto_synchronize import auto_synchronize
from DBSync.data_differences import data_differences
from DBSync.data_fetchers import (
    fetch_mysql_data,
    fetch_postgres_data,
    fetch_data,
    fetch_db_data,
)
from DBSync.data_synchronization import synchronize_tables
from DBSync.utilities import load_yaml


def main():
    source_config = load_yaml("../config/source.yaml")
    target_config = load_yaml("../config/target.yaml")
    mappings_config = load_yaml("../config/mappings.yaml")
    print("SOURCE:::::", source_config)
    print("TARGET:::::", target_config)
    print("MAPPING:::::", mappings_config)
    # fk_mappings = mappings_config.get('foreign_keys', [])
    # reference_db_type = source_config['tables'][0]['db_type']
    # print(reference_db_type)
    # target_db_type = target_config['tables'][0]['db_type']
    # print(target_db_type)

    return

    if not source_config or not target_config or not mappings_config:
        print("Error loading YAML files. Exiting.")
        return

    if reference_db_type == "MySQL":
        reference_conn = pymysql.connect(
            host="localhost",
            user="springstudent",
            password="springstudent",
            database="customer_database",
        )
    if reference_db_type == "PostgreSQL":
        reference_conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="root",
            database="customer_database",
        )

    if target_db_type == "MySQL":
        target_conn = pymysql.connect(
            host="localhost",
            user="springstudent",
            password="springstudent",
            database="customer_database",
        )
    if target_db_type == "PostgreSQL":
        target_conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="root",
            database="customer_database",
        )

    if not reference_conn:
        print("Connection to Reference Database failed.")
        return

    print("Connected to Reference Database successfully.")

    if not target_conn:
        print("Connection to Target Database failed.")
        return
    print("Connected to Target Database successfully.\n")

    for idx, table_info in enumerate(target_config["tables"]):

        start_time = time.time()

        reference_table_info = source_config["tables"][0]
        target_table_info = target_config["tables"][idx]
        key_column_mapping = mappings_config["mappings"][idx]["key_column_mapping"]

        reference_table = reference_table_info["name"]
        reference_key_column = reference_table_info["key_column"]
        reference_columns = reference_table_info["columns"]

        target_table = target_table_info["name"]
        target_key_column = target_table_info["key_column"]
        target_columns = target_table_info["columns"]

        print(
            f"Processing mapping for Reference Table '{reference_table}' and Target Table '{target_table}'..."
        )

        reference_data = fetch_db_data(
            reference_conn, reference_table, reference_columns, reference_db_type
        )
        target_data = fetch_db_data(
            target_conn, target_table, target_columns, target_db_type
        )

        insert_commands, update_commands, insert_mysql_commands = synchronize_tables(
            reference_data,
            target_data,
            reference_key_column,
            target_key_column,
            reference_table,
            target_table,
            key_column_mapping,
            reference_conn,
            target_conn,
            fk_mappings,
            reference_table_info,
            target_table_info,
        )

        data_differences(
            reference_data,
            target_data,
            reference_key_column,
            target_key_column,
            key_column_mapping,
        )

        end_time = time.time()
        total_execution_time = end_time - start_time
        print(f"\nTotal execution time taken: {total_execution_time} seconds.")

        # Write SQL commands to a file
        file_name = f"results/mapping_{idx + 1}_sql_commands.sql"
        with open(file_name, "w") as file:
            file.write("\n".join(insert_commands + update_commands))

        file_name1 = f"results/mapping_{idx + 1}_mysql_commands.sql"
        with open(file_name1, "w") as file1:
            file1.write("\n".join(insert_mysql_commands))

        print(f"\nSQL commands have been written to '{file_name}'.")
        print(f"\nReverse SQL commands have been written to '{file_name1}'.")

        # Ask if user wants to auto synchronize
        auto_sync_choice = input(
            "\nDo you want to auto synchronize the target databases? (yes/no): "
        ).lower()
        if auto_sync_choice == "yes":
            auto_synchronize(file_name, target_conn)
            target_data = fetch_db_data(
                target_conn, target_table, target_columns, target_db_type
            )

        auto_sync_choice = input(
            "\nDo you want to auto synchronize the reference databases? (yes/no): "
        ).lower()
        if auto_sync_choice == "yes":
            auto_synchronize(file_name1, reference_conn)
            reference_data = fetch_db_data(
                reference_conn, reference_table, reference_columns, reference_db_type
            )

    if reference_conn:
        reference_conn.close()
    if target_conn:
        target_conn.close()


if __name__ == "__main__":
    main()
