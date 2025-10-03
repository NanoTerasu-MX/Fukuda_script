import os
import subprocess as sp
import logging as log
import time
import pyinotify

log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

DEST_DIR = 's3://mxdata/mxdata/mxstaff/Data/'
S5CMD = '/data/mxstaff/s3command/bins/s5cmd'
ENDPOINT = 'https://s3ds.cc.tohoku.ac.jp'

def usage():
    print('Usage: python script.py <watch_dir>')
    sys.exit(1)

def put_file(pathfile):

    cmd = [
           S5CMD,
           '--profile', 'default',
           '--endpoint-url=' + ENDPOINT,
           'put',
           filepath, 
           DEST_DIR
           ]

   proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)

   for line in proc.stdout:
       log.info(line.strip())

   proc.wait()
   log.info(f"Upload finished with returncode {proc.returncode}")


class EventHandler(pynotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        if not event.dir:
            log.info(f"File created: {event.pathname}")
            put_file(event.pathname)

    def process_IN_CLOSE_WRITE(self, event):
        if not event.dir:
            log.info(f"File closed after write: {event.pathname}")

    def process_IN_MOVED_TO(self, event):
        if not event.dir:
            log.info(f"File moved into watched dir: {event.pathname}")
            put_file(event.pathname)


def watch_directory(watch_dir):
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE | pynotify.IN_CLOSE_WRITE | pynotify.IN_MOVED_TO
    handler = EventHandler()
    notfier = pyinotify.Notifier(wm, handler)

    wm.add_watch(watch_dir, mask, rec=True, auto_add=True)
    log.info(f"Started watching {watch dir}")

    notifier.loop()


def main():
    if len(sys.argv) < 2:
        usage()

    watch_dir = sys.argv[1]
    
    if not os.path.isdir(watch_dir):
        print(f"{watch_dir} is not a valid directory.")
        sys.exit(1)

    watch_directory(watch_dir)    

if __name__ == '__main__':
    main()
