#!/usr/bin/env python3

from psql_functions import DB, RO_DB
from config import CONFIG


def main():
    uisp_db = RO_DB(CONFIG("uisp_config.json"))
    cache_db = DB(CONFIG("config.json"))

    query = """
            SELECT
                ds.updated_at, 
                d.device_id
                d.hostname, 
                ds.signal_local_60g, 
                ds.signal_remote_60g, 
                    FROM device d 
                        JOIN device_statistics ds 
                        ON d.device_id = ds.device_id 
                        AND ds.signal_local_60g IS NOT NULL 
                            JOIN site st ON d.site_id = st.id 
                                AND st.type = 'site' limit 1;
                                """

    result = uisp_db.execute_query(query)
    for row in result:
        print(row)
        cache_db.insert_data(row)
        


if __name__ == "__main__":
    main()
