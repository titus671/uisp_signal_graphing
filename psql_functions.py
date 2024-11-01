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
        check_exists_uisp_metadata = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'uisp_metadata'
            )
        """
        create_uisp_metadata_table = """
            CREATE TABLE uisp_metadata (
                id UUID PRIMARY KEY,
                name VARCHAR(80),
                model VARCHAR(50),
                type VARCHAR(25),
                role VARCHAR(25),
                site_type VARCHAR(25)
            );
        """
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
                signal_local_60g INT,
                signal_remote_60g INT,
                FOREIGN KEY (device_id) REFERENCES uisp_metadata(id)
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
        self.cursor.execute(check_exists_uisp_metadata)
        if self.cursor.fetchone()[0]:
            self.logger.log("uisp_metadata table exists already")
        else:
            self.cursor.execute(create_uisp_metadata_table)
            self.logger.log("created uisp_metadata table")
        self.cursor.execute(check_exists_uisp_hypertable)
        if self.cursor.fetchone()[0]:
            self.logger.log("uisp_stats hypertable exists already")
        else:
            self.cursor.execute(create_uisp_stats_table)
            self.logger.log("created uisp_stats table")
            self.cursor.execute(create_uisp_stats_hypertable)
            self.logger.log("created uisp_stats hypertable")
    def insert_device(self, device):
        insert_device_query = """
        INSERT INTO uisp_metadata (
            id,
            name,
            model,
            type,
            role,
            site_type
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE
        SET
        name = %s,
        model = %s,
        type = %s,
        role = %s,
        site_type = %s;
        """
        vals = (
                device.id,
                device.name,
                device.model,
                device.type,
                device.role,
                device.site_type,
                device.name,
                device.model,
                device.type,
                device.role,
                device.site_type
                )
        try:
            self.cursor.execute(insert_device_query, vals)
        except psycopg.errors.StringDataRightTruncation:
            for val in vals:
                print(val)
                sys.exit(1)

        return 0
    def insert_stat(self, device):
        print(f"stats for {device.name}")
        base_time = time.time()
        for stat in device.stats_list:
            #print(stat, type(stat))
            t0 = time.time()
                
            #values = (stat["key"], stat["timestamp"], stat["id"], stat["value"])

            query = sql.SQL("""INSERT INTO uisp_stats
                            (time,
                             device_id,
                             {}
                             ) VALUES (to_timestamp({}), {}, {})
                                ON CONFLICT (time, device_id) DO UPDATE
                                SET
                                {} = {} 
                            ;
                            """).format(sql.Identifier(stat["key"].lower()), stat["timestamp"], stat["id"], stat["value"], sql.Identifier(stat["key"].lower()), stat["value"])
            try:
                print("trying")
                self.cursor.execute(query)
                print(self.cursor.fetchone())
            except psycopg.Error as e:
                print("!!!! ERR inserting stats")
                print(stat,"\n")
                print(e)
        print(f"stats inserted in {time.time() - base_time}s")
                
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
