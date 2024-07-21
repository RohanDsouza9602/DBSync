from fetch_indirect_key import fetch_indirect_key


def synchronize_tables(
    reference_data,
    target_data,
    reference_key_column,
    target_key_column,
    reference_table,
    target_table,
    key_column_mapping,
    mysql_conn,
    postgres_conn,
    fk_mappings,
    reference_table_info,
    target_table_info,
):
    insert_sql_commands = []
    update_sql_commands = []
    insert_reference_commands = []

    reference_dict = {row[reference_key_column]: row for row in reference_data}
    target_dict = {row[target_key_column]: row for row in target_data}

    for reference_id, reference_row in reference_dict.items():

        if reference_id in target_dict:
            target_row = target_dict[reference_id]
            update_fields = []
            for mapping in key_column_mapping:
                reference_column = mapping["reference_column"]
                target_column = mapping["target_column"]
                if reference_row[reference_column] != target_row.get(
                    target_column, None
                ):
                    new_value = f"'{reference_row[reference_column]}'"
                    update_fields.append(f"{target_column} = {new_value}")
            if update_fields:
                update_sql_commands.append(
                    f"UPDATE {target_table} SET {', '.join(update_fields)} WHERE {target_key_column} = '{reference_id}';"
                )
            del target_dict[reference_id]
        else:
            insert_values = []
            insert_columns = []
            for mapping in key_column_mapping:
                reference_column = mapping["reference_column"]
                target_column = mapping["target_column"]
                value = reference_row[reference_column]
                insert_columns.append(target_column)
                insert_values.append(f"'{value}'")

            for fk_mapping in fk_mappings:
                target_table = target_table_info["name"]
                if fk_mapping["dependant_table"] == target_table:
                    dependant_foreign_key = fk_mapping["dependant_foreign_key"]
                    dependant_table = fk_mapping["dependant_table"]

                    dependant_value = fetch_indirect_key(
                        postgres_conn, reference_row, fk_mappings, dependant_table
                    )
                    dependant_value = dependant_value[0][0]

                    if dependant_value:
                        insert_columns.append(dependant_foreign_key)
                        insert_values.append(f"'{dependant_value}'")
                    else:
                        print("Dependant foreign key not found.")

            insert_sql_commands.append(
                f"INSERT INTO {target_table} ({', '.join(insert_columns)}) VALUES ({', '.join(insert_values)});"
            )

    for target_id, target_row in target_dict.items():
        insert_values = []
        insert_columns = []
        for mapping in key_column_mapping:
            target_column = mapping["target_column"]
            reference_column = mapping["reference_column"]
            if target_row[target_column] is not None:
                value = target_row[target_column]
                insert_columns.append(reference_column)
                insert_values.append(f"'{value}'")
        insert_reference_commands.append(
            f"INSERT INTO {reference_table} ({', '.join(insert_columns)}) VALUES ({', '.join(insert_values)});"
        )

    return insert_sql_commands, update_sql_commands, insert_reference_commands
