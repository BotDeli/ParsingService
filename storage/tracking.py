from psycopg2.extensions import connection


class TrackingStorage():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS tracking(
            id SERIAL PRIMARY KEY,
            account_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            geo_params TEXT[] NOT NULL,
            created_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
                            
            FOREIGN KEY (account_id) REFERENCES account(id),
            FOREIGN KEY (product_id) REFERENCES product(id)
        );""")

        try:
            self.cursor.execute("ALTER TABLE tracking ADD CONSTRAINT tracking_unique UNIQUE (account_id, product_id);")
        except:
            pass

    def close(self):
        self.cursor.close()

    def start_tracking(self, account_id: int, product_id: int, geo_params: list[str]):
        self.cursor.execute("""INSERT INTO tracking (account_id, product_id, geo_params) 
                            VALUES (%s, %s, %s)
                            ON CONFLICT (account_id, product_id) 
                            DO UPDATE SET geo_params = (
                            SELECT array_agg(DISTINCT value)
                            FROM (
                                SELECT unnest(COALESCE(tracking.geo_params, '{}') || COALESCE(EXCLUDED.geo_params, '{}')) AS value
                            ) AS sub);""",
                            (str(account_id), str(product_id), geo_params))
        
    def stop_tracking(self, account_id: int, product_id: int):
        self.cursor.execute("""DELETE FROM tracking
                            WHERE account_id = %s AND product_id = %s;""",
                            (str(account_id), str(product_id)))

    def is_tracking(self, account_id: int, product_id: int) -> bool:
        self.cursor.execute("""SELECT EXISTS(SELECT 1 FROM tracking 
                            WHERE account_id = %s AND product_id = %s);""",
                            (str(account_id), str(product_id)))
        exists = self.cursor.fetchone()[0]
        return exists

    def get_tracking_geo_params(self, account_id: int, product_id: int) -> list[str]:
        self.cursor.execute("SELECT geo_params FROM tracking WHERE account_id = %s AND product_id = %s;", 
                            (str(account_id), str(product_id)))
        
        result = self.cursor.fetchone()
        if result is None or len(result) == 0:
            return []
        
        return result[0]

    def get_tracking_data(self, account_id: int) -> list[tuple[int, list[str]]]:
        self.cursor.execute("SELECT product_id, geo_params FROM tracking WHERE account_id = %s", (str(account_id),))

        result = self.cursor.fetchall()
        if result is None or len(result) == 0:
            return []

        tracking_data = []
        for row in result:
            tracking_data.append((row[0], row[1]))

        return tracking_data

    def get_tracking_products_id(self, account_id: int) -> list[int]:
        self.cursor.execute("SELECT product_id FROM tracking WHERE account_id = %s;", (str(account_id),))
        
        products_id = []
        result = self.cursor.fetchall()
        for row in result:
            products_id.append(row[0])
        
        return products_id