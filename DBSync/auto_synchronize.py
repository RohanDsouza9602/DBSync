def auto_synchronize(file_name, conn):
    try:
        with open(file_name, 'r') as file:
            commands = file.readlines()
            cursor = conn.cursor()
            for command in commands:
                cursor.execute(command.strip())
            conn.commit()
            print("Auto synchronization completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during auto synchronization: {e}")