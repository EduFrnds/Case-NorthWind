import logging
import pandas as pd

from db.db_config import Config


class Load:
    def __init__(self, db: Config):
        self.db = db

    def upload_dataframe(self, df: pd.DataFrame, table_name: str):
        try:
            logging.info(f"Enviando dados para a tabela '{table_name}'")

            df.to_sql(table_name, self.db.get_engine(), if_exists='replace', index=False)
            logging.info(f"Tabela '{table_name}' atualizada com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao enviar dados para '{table_name}': {e}")