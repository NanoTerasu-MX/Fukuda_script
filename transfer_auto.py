import os
import subprocess as sp
import logging as log
import time
import sys 
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

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

def put_file(pathfile):

    cmd = [
           's3cmd',
           'put',
           '--recursive',
           '--no-check-md5',
           pathfile, 
           DEST_DIR
           ]

    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)

    for line in proc.stdout:
        log.info(line.strip())

    proc.wait()
    log.info(f"Upload finished with returncode {proc.returncode}")

class LoggingEventHandler2(LoggingEventHandler):
    def on_created(self, event):
        log.info(f'{event.src_path} Created')
        put_file(event.src_path)

def initial_upload(watch_dir):
    for root, dirs, files in os.walk(watch_dir):
        for name in files:
            pathfile = os.path.join(root, name)
            proc = sp.Popen(['s3cmd','put','--no-check-md5', pathfile, DEST_DIR])

            for line in proc.stdout:
                log.info(line.strip())

            proc.wait()

def watch(watch_dir):
    event_handler = LoggingEventHandler2()
    observer = Observer()
    observer.schedule(
        event_handler,
        watch_dir,
        recursive=True
        )
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def main():
    if len(sys.argv) < 2:
        usage()

    watch_dir = sys.argv[1]
    
    initial_upload(watch_dir)

    while True:
        watch(watch_dir)

if __name__ == '__main__':
    main()
