def data_differences(reference_data, target_data, reference_key_column, target_key_column, key_column_mapping):
    differences = []
    reference_dict = {row[reference_key_column]: row for row in reference_data}
    target_dict = {row[target_key_column]: row for row in target_data}

    for reference_id, reference_row in reference_dict.items():
        if reference_id in target_dict:
            target_row = target_dict[reference_id]
            diff_found = False
            for mapping in key_column_mapping:
                reference_column = mapping['reference_column']
                target_column = mapping['target_column']
                reference_value = reference_row.get(reference_column)
                target_value = target_row.get(target_column)
                if reference_value is not None and target_value is not None and reference_value != target_value:
                    differences.append(
                        f"Column '{target_column}' differs for ID {reference_id}: Reference Value - {reference_value}, Target Value - {target_value}")
                    diff_found = True
            if diff_found:
                differences.append('')
        else:
            differences.append(f"Row with ID {reference_id} exists in reference table but not in target table.")

    for target_id, target_row in target_dict.items():
        if target_id not in reference_dict:
            differences.append(f"Row with ID {target_id} exists in target table but not in reference table.")

    if differences:
        print("Differences found:")
        print('\n'.join(differences))
        print(f"Total differences found: {len(differences) - sum(1 for line in differences if line.strip() == '')}")
    else:
        print("No differences found.")