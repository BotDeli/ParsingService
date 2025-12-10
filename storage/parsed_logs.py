from psycopg2.extensions import connection
from structs.product import ProductInfo


class ParsedLogsStorage():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS parsed_logs(
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL, 
            geo VARCHAR(255) NOT NULL,
            status VARCHAR(255) NOT NULL,
            http_status INTEGER NOT NULL,
            error_message VARCHAR(255),
            error_details TEXT,
            created_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
            
            FOREIGN KEY (product_id) REFERENCES product(id)
        );""")

    def save_parsed_log(self, product_id: int, product: ProductInfo):
        self.cursor.execute("""INSERT INTO parsed_logs(product_id, geo, status, http_status, error_message, error_details)
                                VALUES(%s, %s, %s, %s, %s, %s);""",
                                (product_id, product.location, product.status, product.http_status, product.err_message, str(product.err_details)))

    def close(self):
        self.cursor.close()
