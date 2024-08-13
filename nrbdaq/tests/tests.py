import os
import unittest
from nrbdaq.utils.utils import load_config
from nrbdaq.utils.sftp import SFTPClient
import nrbdaq.instr.avo as avo

config = load_config('nrbdaq.cfg')

class TestSFTP(unittest.TestCase):
    def test_config_host(self):
        self.assertEqual(config['sftp']['host'], 'sftp.meteoswiss.ch')

    def test_is_alive(self):
        sftp = SFTPClient(config=config)

        self.assertEqual(sftp.is_alive(), True)

    def test_transfer_single_file(self):
        sftp = SFTPClient(config=config)

        testfile = 'hello_world.txt'
        localpath = 'nrbdaq/data/tests'
        remotepath = '.'
       
        if sftp.remote_item_exists(remotepath):
            sftp.remove_remote_file(remotepath)            

        sftp.put_file(localpath=os.path.join(localpath, testfile), remotepath=remotepath)

        self.assertEqual(sftp.remote_item_exists(os.path.join(remotepath, testfile)), True)

        # clean up
        sftp.remove_remote_file(os.path.join(remotepath, testfile))


class TestAVO(unittest.TestCase):
    def test_get_data(self, url=config['AVO']['url_nairobi']):
        file_path = os.path.join(os.path.expanduser(config['data']['root']), 'avo')
        data = avo.get_data(url=url)
        self.assertEqual(list(data.keys()), ['historical', 'name', 'current'])

if __name__ == "__main__":
    unittest.main(verbosity=2)
    