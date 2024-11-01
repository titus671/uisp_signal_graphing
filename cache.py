#!/usr/bin/env python3
from psql_functions import DB, RO_DB
from config import CONFIG
import time, argparse

def loop_main():
    while True:
        try:
            main()
            time.sleep(10)
        except Exception as e:
            import sys
            print(e)
            sys.exit(1)

def main():
    uisp_db = RO_DB(CONFIG("uisp_config.json"))
    cache_db = DB(CONFIG("config.json"))

    cache_db.create_tables()

    query = """
            SELECT
                ds.updated_at, 
                d.device_id,
                d.hostname, 
                ds.signal_local_60g, 
                ds.signal_remote_60g 
                    FROM device d 
                        JOIN device_statistics ds 
                        ON d.device_id = ds.device_id 
                        AND ds.signal_local_60g IS NOT NULL 
                            JOIN site st ON d.site_id = st.id 
                                AND st.type = 'site';
                                """

    result = uisp_db.execute_query(query)
    for row in result:
        cache_db.insert_data(row)
        
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--loop", action="store_true", help="loop")
    args = parser.parse_args()

    if args.loop:
        loop_main()
    else:
        main()


if __name__ == "__main__":
    run()
