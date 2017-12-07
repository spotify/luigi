import itertools
import threading
import unittest

from luigi.contrib.hdfs import get_autoconfig_client


class HdfsClientTest(unittest.TestCase):
    def test_get_autoconfig_client_cached(self):
        original_client = get_autoconfig_client()
        for _ in range(100):
            self.assertIs(original_client, get_autoconfig_client())

    def test_threaded_clients_different(self):
        clients = []

        def add_client():
            clients.append(get_autoconfig_client())

        # run a bunch of threads to get new clients in them
        threads = [threading.Thread(target=add_client) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for client1, client2 in itertools.combinations(clients, 2):
            self.assertIsNot(client1, client2)
