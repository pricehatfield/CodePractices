import unittest
import re
from glob import glob
from os import path, remove, listdir
from UnicodeRemoval import threads

class RemovalWorker_Test(unittest.TestCase):
    def setUp(self):
        self.source_file_path = path.abspath(path.join('.', 'tests', 'test_data', 'source', 'unicode_string.txt'))
        self.target_file_path = path.abspath(path.join('.', 'tests', 'test_data', 'results', 'unicode_string.txt'))
        self.pattern = r'[^\x00-\x7f]'
        self.replacement = r''

    def tearDown(self):
        pass

    def test_file_available(self):
        assert(path.exists(self.source_file_path))

    def test_unicode_target_replacement(self):
        my_worker = threads.RemovalWorker(
                        self.source_file_path
                        , self.target_file_path
                        , self.pattern
                        , self.replacement)
        my_worker.start()
        my_worker.join()
        with open(self.target_file_path, 'r') as out_file:
            assert(len(re.findall(self.pattern, out_file.read())) == 0)

class RemovalController_Test(unittest.TestCase):
    def setUp(self):
        self.file_name = path.abspath(path.join('.', 'tests', 'test_data', 'source', 'unicode_string.txt'))
        self.source_folder_path = path.abspath(path.join('.', 'tests', 'test_data', 'source'))
        self.target_folder_path = path.abspath(path.join('.', 'tests', 'test_data', 'results'))
        self.pattern = r'[^\x00-\x7f]'
        self.replacement = r''
        self.source_files = listdir(self.source_folder_path)

    def tearDown(self):
        pass

    def test_folder_available(self):
        assert(len(self.source_files) > 0)

    def test_controller_iteration(self):
        #purge target directory
        for f in glob('{0}/*'.format(self.target_folder_path)):
            remove(f)

        my_controller = threads.RemovalController(self.source_folder_path
                        , self.target_folder_path
                        , self.pattern
                        , self.replacement)
        my_controller.start()
        my_controller.join()

        #Verify inbound and outbound file lists match
        target_files = listdir(self.target_folder_path)
        assert(self.source_files == target_files)

if __name__ == "__main__":
    unittest.main()