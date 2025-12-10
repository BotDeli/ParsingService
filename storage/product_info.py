from psycopg2.extensions import connection
from structs.product import ProductInfo


class ProductInfoStorage():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS product_info(
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL,
            geo VARCHAR(255) NOT NULL,
            price INTEGER,
            original_price INTEGER,
            currency VARCHAR(8),
            aviable BOOLEAN NOT NULL,
            parsed_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
                            
            FOREIGN KEY (product_id) REFERENCES product(id)
        );""")

    def save_product_info(self, product_id: int, product: ProductInfo):
        self.cursor.execute("""INSERT INTO product_info(product_id, geo, price, original_price, currency, aviable)
                                VALUES(%s, %s, %s, %s, %s, %s);""",
                                (product_id, product.location, product.price, product.original_price, product.currency, product.available))

    def close(self):
        self.cursor.close()
