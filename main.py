import logging
import os

from data.extract import Extract
from data.load import Load
from db import db_config
from db.db_config import Config, Connection


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
    logging.info("Iniciando processo de extração e carga de dados...")

    # Testar a conexão com o banco de dados
    with Connection() as db:
        db.execute("SHOW client_encoding;")
        encoding_result = db.fetchall()
        logging.info(f"Codificação do banco de dados: {encoding_result[0][0]}")  # Deve ser UTF-8

    # Caminho da pasta de arquivos CSV
    folder_path = r"C:\\Users\\Eduardo\\Desktop\\dados northwind"

    # Extração de arquivos CSV
    logging.info("Iniciando extração de arquivos CSV...")
    extract = Extract(folder_path)
    csv_files = extract.get_csv_files()

    # Criando a conexão com o banco para carga
    db_config = Config()
    load = Load(db_config)

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        table_name = os.path.splitext(file)[0]

        # Lendo CSV
        df = extract.read_csv(file_path)
        if not df.empty:
            logging.info(f"Carregando dados para a tabela '{table_name}'...")
            load.upload_dataframe(df, table_name)
        else:
            logging.warning(f"O arquivo '{file}' está vazio e será ignorado.")

    logging.info("Processo concluído com sucesso.")
