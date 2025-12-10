import unittest
from storage.postgresql import MainStorage

TEST_IP_ADDR = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
PSQL_CONFIG = {
    "database": "postgres",
    "user": "kare",
    "password": "",
    "host": "127.0.0.1",
    "port": 5432,
}

class TestServerAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = MainStorage(PSQL_CONFIG)
        cls.storage.init_tables()
        cls.storage.ban_list.unban_ip(TEST_IP_ADDR)

    @classmethod
    def tearDownClass(cls):
        cls.storage.close()

    def test_working_ban_list(self):
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(False, is_banned)

        self.storage.ban_list.ban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(True, is_banned)

        self.storage.ban_list.unban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(False, is_banned)

    def test_load_ban_list(self):
        self.storage.ban_list.ban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(True, is_banned)

        self.storage.ban_list._is_banned = dict()
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(False, is_banned)

        self.storage.ban_list._load_ban_list()
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(True, is_banned)

        self.storage.ban_list.unban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(False, is_banned)

    def test_errors_ban_list(self):
        self.storage.ban_list.ban_ip(TEST_IP_ADDR)
        self.storage.ban_list.ban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(True, is_banned)

        self.storage.ban_list.unban_ip(TEST_IP_ADDR)
        self.storage.ban_list.unban_ip(TEST_IP_ADDR)
        is_banned = self.storage.ban_list.is_banned_ip(TEST_IP_ADDR)
        self.assertEqual(False, is_banned)


if __name__ == "__main__":
    unittest.main()