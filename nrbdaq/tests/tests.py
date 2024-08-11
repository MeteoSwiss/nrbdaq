import unittest
from nrbdaq.utils.utils import load_config
from nrbdaq.utils.sftp import SFTPClient

config = load_config('nrbdaq.cfg')

class TestSFTP(unittest.TestCase):
    def test_config_host(self):
        self.assertEqual(config['sftp']['host'], 'sftp.meteoswiss.ch')

    # def test_config_key(self):

    #     self.assertEqual(config['sftp']['key'], 'sftp.meteoswiss.ch')

    def test_is_alive(self):
        sftp = SFTPClient(config=config)

        self.assertEqual(sftp.is_alive(), True)

    def test_transfer_single_file(self):
        sftp = SFTPClient(config=config)

        if sftp.remote_item_exists('hello_world.txt'):
            print('Please remove file from remote before running this test.')

        sftp.put_file(localpath='nrbdaq/tests/hello_world.txt', remotepath='.')

        self.assertEqual(sftp.remote_item_exists('hello_world.txt'), True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
    