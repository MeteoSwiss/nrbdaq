import os
import unittest
from nrbdaq.utils.utils import load_config
from nrbdaq.utils.sftp import SFTPClient
import nrbdaq.instr.avo as avo

config = load_config('nrbdaq.yaml')

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
    def test_download_data(self):
        data = avo.download_data(url=config['AVO']['urls']['url_nairobi'])
        self.assertEqual(list(data.keys()), ['historical', 'name', 'current'])

    def test_data_to_dfs(self):
        data = avo.download_data(url=config['AVO']['urls']['url_nairobi'])
        station, dfs = avo.data_to_dfs(data=data, 
                              file_path=os.path.join(os.path.expanduser(config['root']), config['AVO']['data']),
                              staging=os.path.join(os.path.expanduser(config['root']), config['AVO']['staging']))
        self.assertEqual(station, 'kmd_hq_nairobi')

if __name__ == "__main__":
    unittest.main(verbosity=2)
    