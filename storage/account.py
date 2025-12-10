from psycopg2.extensions import connection
import secrets
import base64


API_KEY_LENGTH = 64


#   EMAIL        |       NAME
# -----------------------------------
#                    -> test1_name
#                    |
#                    |
#   test@mail.ru ----|
#                    |
#                    |
#                    -> test2_name
#
#   Одна электронная почта может иметь несколько разных аккаунтов
#   На каждый аккаунт приходится свое уникальное имя
#   На каждое имя приходится свой уникальный api_key

class AccountStorage():
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def init_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS account(
            id SERIAL PRIMARY KEY,
            api_key VARCHAR(255) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP)
        );""")
        
    # @return api_key
    def create_account(self, email: str, name: str) -> str:
        if not is_valid_email(email):
            raise ValueError("email address is not valid")

        if self.is_exists_name(name) or 254 < len(name):
            raise ValueError("name already created")
        
        for i in range(3):
            try:
                api_key = generate_api_key()
                self.cursor.execute("""INSERT INTO account(api_key, email, name)
                                        VALUES(%s, %s, %s);""",
                                        (api_key, email, name))
                return api_key
            except:
                continue

        raise ValueError("fail generate api key")
        
    def is_exists_name(self, name: str) -> bool:
        self.cursor.execute("SELECT EXISTS(SELECT 1 FROM account WHERE name = %s);", (str(name),))
        exists = self.cursor.fetchone()[0]
        return exists
    
    def get_account_id(self, api_key: str) -> int | None:
        self.cursor.execute("SELECT id FROM account WHERE api_key = %s;", (str(api_key),))
        result = self.cursor.fetchone()
        if result is None or len(result) == 0:
            return None

        account_id = result[0]
        return account_id
    
    def close(self):
        self.cursor.close()

def generate_api_key() -> str:
    b = secrets.token_bytes(API_KEY_LENGTH)
    api_key = base64.urlsafe_b64encode(b).decode('utf-8')
    return api_key

import re
MAX_EMAIL_LENGTH = 254
email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$'
def is_valid_email(email: str) -> bool:
        email = str(email)
        if MAX_EMAIL_LENGTH < len(email):
            return False

        return bool(re.match(email_pattern, email))