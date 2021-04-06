import sys
import unittest
import kazoo.client as kzcl
import host_ip_provider as hip


class TestZKClientServiceMethods(unittest.TestCase):
    def setUp(self):
        self.kzclient = kzcl.KazooClient('127.0.0.1:2181')

    def test_create_non_emphemeral(self):
        node_path = '/my/znode/n_'
        create_node_path = self.kzclient.create(node_path, hip.get_host_ip(), makepath=True, ephemeral=False,
                                                sequence=True)
        self.assertTrue(len(create_node_path) == len(node_path) + 10)

    def test_create_emphemeral_node(self):
        node_path = '/my/znode/n_'
        create_node_path = self.kzclient.create(node_path, hip.get_host_ip(), makepath=True, ephemeral=True,
                                                sequence=True)
        self.assertTrue(len(create_node_path) == len(node_path) + 10)


if __name__ == '__main__':
    unittest.main()
