import os
import logging
import psycopg2 as pdb
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        self.db_name = os.getenv("POSTGRES_DB_NAME")
        self.db_user = os.getenv("POSTGRES_DB_USER")
        self.db_pass = os.getenv("POSTGRES_DB_PASS")
        self.db_host = os.getenv("POSTGRES_DB_HOST")
        self.db_port = os.getenv("POSTGRES_DB_PORT")
        self.config = {
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_pass,
            "host": self.db_host,
            "port": self.db_port,
            "options": "-c client_encoding=UTF-8"
        }
        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}?client_encoding=UTF-8"
        )

    def get_engine(self):
        return self.engine


class Connection(Config):
    def __init__(self):
        super().__init__()
        try:
            self.conn = pdb.connect(**self.config)
            self.cur = self.conn.cursor()
        except Exception as e:
            logging.error(f"Erro ao conectar: {e}")
            exit()

    def close(self):
        try:
            if self.conn:
                self.cur.close()
                self.conn.close()
                logging.info("Conexão encerrada com sucesso")
        except Exception as e:
            logging.error(f"Erro ao encerrar a conexão: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()
        self.close()

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def fetchall(self):
        return self.cur.fetchall()

    def execute(self, sql, params=None):
        self.cur.execute(sql, params or ())

    def query(self, sql, params=None):
        self.cur.execute(sql, params or ())
        return self.fetchall()
