from psycopg2.extensions import connection


class RedisStorage():
    def __init__(self):
        pass


class BanListIP():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = self.conn.cursor()

        self._is_banned = dict()
        self._load_ban_list()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS ban_list(
            id SERIAL PRIMARY KEY,
            ip_addr VARCHAR(64) UNIQUE NOT NULL,
            banned_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP)
        );""")

    def _load_ban_list(self):
        try:
            self.cursor.execute("SELECT ip_addr FROM ban_list;")
            
            result = self.cursor.fetchall()
            if result is None or len(result) == 0:
                return
            
            for row in result:
                self._is_banned[str(row[0])] = True
        except:
            pass

    def is_banned_ip(self, addr: str) -> bool:
        return self._is_banned.get(addr, False)

    def ban_ip(self, addr: str):
        try:
            self._is_banned[addr] = True
            self.cursor.execute("INSERT INTO ban_list(ip_addr) VALUES(%s);", (str(addr),))
            return True
        except:
            return False

    def unban_ip(self, addr: str):
        try:
            del self._is_banned[addr]
            self.cursor.execute("DELETE FROM ban_list WHERE ip_addr = %s;", (str(addr),))
            return True
        except:
            return False

    def get_banned_ip_list(self) -> list[str]:
        return list(self._is_banned.keys())

    def close(self):
        self.cursor.close()