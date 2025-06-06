import os
from datetime import datetime
import mysql.connector
import csv

from config import DB_CONFIG RESPALDO_FOLDER

db_name = DB_CONFIG["database"]

# Ruta donde se almacenarán las copias de seguridad
os.makedirs(RESPALDO_FOLDER, exist_ok=True)

def log_message(message):
    print(f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')} - {message}")

def backup_database():
    now = datetime.now()
    backup_folder = f"Respaldo_Del_{now.strftime('%d-%m-%Y_%H-%M-%S')}"
    backup_path = os.path.join(RESPALDO_FOLDER, backup_folder)
    os.makedirs(backup_path, exist_ok=True)

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        sql_backup_path = os.path.join(backup_path, f"{db_name}_backup.sql")
        with open(sql_backup_path, 'w', encoding='utf-8') as sql_file:
            sql_file.write(f"-- Copia de seguridad de la base de datos: {db_name}\n")
            sql_file.write(f"-- Fecha: {now.strftime('%d-%m-%Y %H:%M:%S')}\n\n")

            for table in tables:
                table_name = table[0]

                cursor.execute(f"SHOW CREATE TABLE {table_name}")
                create_table_sql = cursor.fetchone()[1]
                sql_file.write(f"{create_table_sql};\n\n")

                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if rows:
                    sql_file.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES \n")
                    values_list = []
                    for row in rows:
                        values = ", ".join(
                            [f"'{str(value).replace('\\', '\\\\').replace('\'', '\\\'')}'" if value is not None else "NULL" for value in row]
                        )
                        values_list.append(f"({values})")
                    sql_file.write(",\n".join(values_list) + ";\n\n")

                with open(os.path.join(backup_path, f"{table_name}.csv"), 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(columns)
                    writer.writerows(rows)

        log_message(f"Copia de seguridad creada en: {backup_path}")
        cursor.close()
        conn.close()
        return backup_folder
    except mysql.connector.Error as err:
        log_message(f"Error al crear la copia de seguridad: {err}")
        return None

def clean_old_backups(exclude_folder):
    now = datetime.now()
    current_month = now.strftime('%Y-%m')
    for folder in os.listdir(RESPALDO_FOLDER):
        folder_path = os.path.join(RESPALDO_FOLDER, folder)
        if os.path.isdir(folder_path) and folder != exclude_folder:
            try:
                folder_date = folder.split('_')[2]
                folder_month = '-'.join(folder_date.split('-')[1::-1])
                if folder_month != current_month:
                    for filename in os.listdir(folder_path):
                        os.remove(os.path.join(folder_path, filename))
                    os.rmdir(folder_path)
            except IndexError:
                continue
    log_message("Archivos antiguos del mes eliminados.")

if __name__ == "__main__":
    log_message("Iniciando proceso de respaldo y limpieza...")
    backup_folder = backup_database()
    if backup_folder:
        clean_old_backups(backup_folder)
    log_message("Proceso finalizado.")
