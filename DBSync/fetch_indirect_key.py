from data_fetchers import fetch_data


def fetch_indirect_key(conn, reference_row, fk_mappings, dependant_table):
    # Find the foreign key mapping for the given dependant_table
    fk_mapping = next(
        (fk for fk in fk_mappings if fk["dependant_table"] == dependant_table), None
    )
    if fk_mapping is None:
        print("Foreign key mapping not found.")
        return

    master_reference = fk_mapping["reference_via"]["master_reference"]
    master_value = reference_row[master_reference]

    # Check if 'dependant_reference' key exists in the mapping
    if "dependant_reference" not in fk_mapping["reference_via"]:
        print("Dependant reference key not found in the mapping.")
        return

    dependant_reference = fk_mapping["reference_via"]["dependant_reference"]
    dependant_foreign_key = fk_mapping["dependant_foreign_key"]

    query = f"SELECT {fk_mapping['master_key']} FROM {fk_mapping['master_table']} WHERE {dependant_reference} = '{master_value}' LIMIT 1;"
    print(query)
    result = fetch_data(conn, query, db_type="postgres")

    if result:
        return result
    else:
        print("Dependant foreign key not found.")
        return None
