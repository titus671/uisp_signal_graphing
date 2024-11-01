#!/usr/bin/env python
from typing import List
import psycopg, sys, json
from config import CONFIG
from logger import Logger
from psycopg import sql
import concurrent.futures, time

class DB:
    def __init__(self, config: CONFIG):

        self.logger = Logger()
        try:
            self.conn = psycopg.connect(f"postgres://{config.db_username}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_name}")
            self.cursor = self.conn.cursor()
        except AttributeError:
            self.logger.log("Connection was not valid,\nconsider double checking your creds")
            sys.exit(1)

    def create_tables(self):
        check_exists_uisp_hypertable = """
            SELECT EXISTS (
                SELECT 1
                FROM _timescaledb_catalog.hypertable
                WHERE table_name = 'uisp_stats'
            )
            """
        create_uisp_stats_table = """
            CREATE TABLE uisp_stats (
                time TIMESTAMPTZ NOT NULL,
                device_id UUID,
                name VARCHAR(25),
                signal_local_60g INT,
                signal_remote_60g INT,
            );
            CREATE INDEX ON uisp_stats (device_id, time DESC);
            ALTER TABLE uisp_stats
            ADD CONSTRAINT unique_id_time_constraint UNIQUE (device_id, time);
            """
        create_uisp_stats_hypertable = """
            SELECT create_hypertable('uisp_stats', 'time');
            ALTER TABLE uisp_stats SET (timescaledb.compress,
                                         timescaledb.compress_segmentby = 'device_id');
            SELECT add_compression_policy('uisp_stats', INTERVAL '2 days');
            """
        self.cursor.execute(check_exists_uisp_hypertable)
        if self.cursor.fetchone()[0]:
            self.logger.log("uisp_stats hypertable exists already")
        else:
            self.cursor.execute(create_uisp_stats_table)
            self.logger.log("created uisp_stats table")
            self.cursor.execute(create_uisp_stats_hypertable)
            self.logger.log("created uisp_stats hypertable")

    def insert_data(self, data):
        data_q = """
        INSERT INTO uisp_stats
            (time,
            device_id,
            name,
            signal_local_60g,
            signal_remote_60g
            )
                VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(data_q, data)
    
    def __del__(self):
        self.conn.commit()
        self.cursor.close()

class RO_DB:
    def __init__(self, config: CONFIG):
        self.logger = Logger()
        try:
            self.conn = psycopg.connect(f"postgres://{config.db_username}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_name}")
            self.cursor = self.conn.cursor()
        except AttributeError:
            self.logger.log("Connection was not valid,\nconsider double checking your creds")
            sys.exit(1)

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def __del__(self):
        self.cursor.close()


def main():
    db = DB(CONFIG("config.json"))
    db.create_tables()

if __name__ == "__main__":
    main()
