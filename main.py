import logging
import os

from data.extract import Extract
from data.load import Load
from db import db_config
from db.db_config import Config


def test_connection():
    try:
        with db_config.Connection() as conn:
            conn.execute("SELECT current_database();")
            db_name = conn.fetchall()
            print(f"Conectado ao banco de dados: {db_name[0][0]}")
    except Exception as e:
        logging.error(f"Erro ao testar a conexão: {e}")


def upload_to_postgres():
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    folder_path = r"C:\\Users\\Eduardo\\Desktop\\dados northwind"

    logging.info("Iniciando extração de arquivos CSV...")
    extract = Extract(folder_path)
    csv_files = extract.get_csv_files()

    db = Config()
    load = Load(db)

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        table_name = os.path.splitext(file)[0]
        df = extract.read_csv(file_path)
        load.upload_dataframe(df, table_name)

    logging.info("Processo concluído com sucesso.")
