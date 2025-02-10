import logging
import os
import pandas as pd


class Extract:
    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def get_csv_files(self):
        return [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]

    @staticmethod
    def read_csv(file_path: str):

        try:
            return pd.read_csv(file_path, encoding='UTF-8', sep=';', on_bad_lines='skip', skip_blank_lines=True)
        except Exception as e:
            logging.error(f"Erro ao ler arquivo {file_path}: {e}")
            return pd.DataFrame()

