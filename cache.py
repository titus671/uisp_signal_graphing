#!/usr/bin/env python3
from psql_functions import DB, RO_DB
from config import CONFIG
import time, argparse

def loop_main(args):
    while True:
        try:
            main(args)
            time.sleep(10)
        except Exception as e:
            import sys
            print(e)
            sys.exit(1)

def main(args):
    uisp_db = RO_DB(CONFIG("uisp_config.json"), args)
    cache_db = DB(CONFIG("config.json"), args)

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
    parser.add_argument("-v", "--verbosity", default=0, action="store_true", help="increase output verbosity")
    args = parser.parse_args()

    if args.loop:
        loop_main(args)
    else:
        main(args)


if __name__ == "__main__":
    run()
