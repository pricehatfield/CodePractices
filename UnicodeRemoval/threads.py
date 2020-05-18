from portalocker import portalocker
import threading
from time import sleep
import glob
from os.path import isfile, join, abspath
import io
import re

class RemovalWorker(threading.Thread):
    def __init__(self, source_file, target_file, search_pattern, replacement):
        threading.Thread.__init__(self, name='worker.{0}'.format(source_file))
        self.source_file = source_file
        self.target_file = target_file
        self.search_pattern = search_pattern
        self.replacement = replacement
        self.is_running = True

    def __exit__(self, exc_type, exc_value, traceback):
        self.is_running = False

    def stop(self):
        self.is_running = False

    def run(self):
        while self.is_running:
            try:
                with open(self.source_file, 'r', encoding='latin-1') as inbound:
                    #create a lock so other threads cannot pull the file
                    portalocker.lock(inbound, flags=portalocker.constants.LOCK_EX)
                    with open(self.target_file, 'w') as outbound:
                        for line in inbound.readline():
                            #regex undesired characters from each line and write out
                            new_string = re.sub(self.search_pattern, self.replacement, line)
                            outbound.writelines(new_string)
            except portalocker.exceptions.AlreadyLocked:
                #Ignore; let the other thread handle this file
                pass
            except portalocker.exceptions.BaseLockException as e:
                print('Unable to acquire lock; args:[{0}]'.format(e.args))
            except Exception as e:
                print('Failure converting file: [{0}]'.format(e))
            finally:
                self.is_running = False


class RemovalController(threading.Thread):
    def __init__(self, source_directory, target_directory, search_pattern, replacement):
        threading.Thread.__init__(self, name='controller.{0}'.format(source_directory))
        self.source_directory = source_directory
        self.target_directory = target_directory
        self.search_pattern = search_pattern
        self.replacement = replacement
        self.is_running = True
        self.queue_complete = False
        self.all_threads = []

    def __exit__(self, exc_type, exc_value, traceback):
        self.is_running = False

    def stop(self):
        self.is_running = False

    def run(self):
        glob_path = join(self.source_directory, '*')
        while self.is_running:
            while not self.queue_complete:
                print('Scanning {0} ...'.format(glob_path))
                for filename in glob.glob(glob_path, recursive=True):
                    #verify not a directory
                    if isfile(filename):
                        print('Spawning worker thread for {0} ...'.format(filename))
                        worker = RemovalWorker(filename
                                , re.sub(self.source_directory, self.target_directory, filename)
                                , self.search_pattern
                                , self.replacement)
                        self.all_threads.append(worker)
                        worker.start()
                    else:
                        print('Skipping directory {0} ...'.format(filename))
                self.queue_complete = True
                print('All files queued for processing ...')
            
            #wait ten seconds then check to see if any workers are still busy
            threads_working = False
            for t in self.all_threads:
                threads_working = threads_working or t.is_alive()
            #if no threads are still working, turn of our loop
            if not threads_working:
                self.is_running = False
                print('Clean exit; all threads finished work.')
            else:
                sleep(10)
        
        #if interrupt is requested, pass through the termination to worker threads still running
        for t in self.all_threads:
            if t.is_alive():
                print('Stopping thread {0}.'.format(t.name()))
                t.stop()
                t.join()

if __name__ == "__main__":
    path_to_inbound = join(".", "tests", "test_data", "source")
    path_to_outbound = join(".", "tests", "test_data", "results")
    regex_match_string = r'[^\x00-\x7f]'
    regex_replacement_string = r''

    my_controller = RemovalController(path_to_inbound
                    , path_to_outbound
                    , regex_match_string
                    , regex_replacement_string)
    my_controller.run()

    