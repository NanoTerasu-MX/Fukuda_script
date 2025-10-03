import os
import subprocess as sp
import logging as log
import time
import sys 
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )


DEST_DIR = 's3://mxdata/mxdata/mxstaff/test/'

def usage():
    print('Usage: python script.py <watch_dir>')
    sys.exit(1)

def put_file(pathfile, dirpath):

    cmd = [
           's3cmd',
           'put',
           '--recursive',
           '--no-check-md5',
           pathfile, 
           DEST_DIR + dirpath
           ]

    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)

    log.info(f"""stdout: {proc.stdout}\n
               stderr: {proc.stderr}""")

    proc.wait()
    log.info(f"Upload finished with returncode {proc.returncode}")

class WatchEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        log.info(f'{event.src_path} Created')
        dirpath = os.path.dirname(event.src_path)
        put_file(event.src_path, dirpath)

    def on_modified(self, event):
        log.info(f'{event.src_path} Changed')
        dirpath = os.path.dirname(event.src_path)
        put_file(event.src_path, dirname)
            

'''
class UploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            log.info(f"File created: {event.src_path}")
            self.upload(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            log.info(f"File modified: {event.src_path}")
            self.upload(event.src_path)

    def upload(self, pathfile):
        cmd = ['s3cmd', 'put', '--recursive', '--no-check-md5', pathfile, DEST_DIR]
        log.info(f"Starting upload: {pathfile}")

        sp.Popen(cmd, stdout=open('transfer.log', 'a'), stderr=subprocess.STDOUT)
'''

def initial_upload(watch_dir):
    proc = sp.Popen(['s3cmd','sync','--recursive','--no-check-md5', watch_dir, DEST_DIR],
                             stdout=sp.PIPE, stderr=sp.STDOUT, text=True)

    for line in proc.stdout:
        log.info(line.strip())

    proc.wait()

def watch(watch_dir):
    event_handler = WatchEventHandler()
    observer = PollingObserver()
    observer.schedule(
        event_handler,
        watch_dir,
        recursive=True
        )
    observer.start()
    log.info(f"Starting watching {watch_dir}")
    
    try:
        while True:
            log.info("Sleep...")
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        log.info("Stop!!!")

    observer.join()

def main():
    if len(sys.argv) < 2:
        usage()

    watch_dir = sys.argv[1]
    
    initial_upload(watch_dir)

    watch(watch_dir)

if __name__ == '__main__':
    main()
