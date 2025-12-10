from psycopg2.extensions import connection


class ProductStorage():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS product(
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            article VARCHAR(255) NOT NULL,
            source VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP)
        );""")

    def close(self):
        self.cursor.close()

    def save_product(self, url: str, article: str, source: str) -> int:
        self.cursor.execute("""INSERT INTO product(url, article, source) 
                            VALUES (%s, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                            RETURNING id;""",
                            (url, article, source))
        
        id_data = self.cursor.fetchone()
        if id_data is None:
            return self.get_product_id(article, source)
        
        return id_data[0]

    def get_product_id(self, article: str, source: str) -> int | None:
        self.cursor.execute("SELECT id FROM product WHERE source=%s AND article=%s", (source, article))
        id = self.cursor.fetchone()
        if id is None:
            return None
        
        return id[0]

    def get_product_url(self, id: int) -> str:
        self.cursor.execute("SELECT url FROM product WHERE id = %s LIMIT 1", (str(id),))
        url = self.cursor.fetchone()[0]
        return url
    
    