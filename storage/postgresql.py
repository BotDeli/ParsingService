import psycopg2

from storage.ban_list import BanListIP
from storage.product import ProductStorage
from storage.product_info import ProductInfoStorage
from storage.parsed_logs import ParsedLogsStorage
from storage.account import AccountStorage
from storage.tracking import TrackingStorage


class MainStorage():
    def __init__(self, cfg_postgresql: dict):
        self.conn = psycopg2.connect(
            database=cfg_postgresql["database"],
            user=cfg_postgresql["user"],
            password=cfg_postgresql["password"],
            host=cfg_postgresql["host"],
            port=cfg_postgresql["port"]
        )
        self.conn.autocommit = True
        
        self.ban_list = BanListIP(self.conn)
        self.product = ProductStorage(self.conn)
        self.product_info = ProductInfoStorage(self.conn)
        self.parsed_logs = ParsedLogsStorage(self.conn)
        self.account = AccountStorage(self.conn)
        self.tracking = TrackingStorage(self.conn)

    def init_tables(self):
        self.ban_list.init_table()
        self.product.init_table()
        self.product_info.init_table()
        self.parsed_logs.init_table()
        self.account.init_table()
        self.tracking.init_table()

    def export_prices_data(self, account_id: int) -> list:
        cursor = self.conn.cursor()
        cursor.execute("""SELECT DISTINCT ON(pr.id, geo) inf.product_id, pr.url, inf.geo, inf.price, inf.original_price, inf.parsed_at, pr.source
                        FROM tracking as tr
                        LEFT JOIN product as pr
                        ON tr.product_id = pr.id
                        LEFT JOIN product_info as inf
                        ON inf.product_id = pr.id
                        WHERE tr.account_id = %s
                        AND (
                            inf.geo = ANY(tr.geo_params)
                            OR inf.geo = 'none'
                        )

                        ORDER BY pr.id, geo, parsed_at DESC;""", (account_id,))

        result = cursor.fetchall()
        if result is None or len(result) == 0:
            return []

        prices_data = []
        for row in result:
            prices_data.append({
                "product_id": int(row[0]),
                "url": row[1],
                "geo": row[2],
                "price": row[3],
                "old_price": row[4],
                "datetime": row[5],
                "source": row[6],
            })

        cursor.close()

        return prices_data
    
    def get_required_update_geo_params(self) -> list[tuple[int, list[str]]]:
        cursor = self.conn.cursor()
        cursor.execute("""SELECT p.url, tmp.required_update_geo_params
                        FROM (
                            SELECT 
                            tr.product_id,
                            ARRAY(
                                SELECT DISTINCT geo_elem
                                FROM UNNEST(tr.geo_params) AS geo_elem
                                WHERE geo_elem NOT IN (
                                SELECT DISTINCT geo
                                FROM product_info
                                WHERE product_id = tr.product_id 
                                    AND DATE(parsed_at) = CURRENT_DATE
                                )
                            ) AS required_update_geo_params
                            FROM tracking as tr
                        ) as tmp
                        LEFT JOIN product as p
                        ON p.id = tmp.product_id
                        WHERE 0 < ARRAY_LENGTH(required_update_geo_params, 1)""")
        
        results = cursor.fetchall()
        if results is None or len(results) == 0:
            return []
        
        required_update = []
        for row in results:
            required_update.append((row[0], row[1]))

        return required_update
            
    def close(self):
        self.ban_list.close()
        self.product.close()
        self.product_info.close()
        self.parsed_logs.close()
        self.account.close()
        self.tracking.close()
        
        self.conn.close()