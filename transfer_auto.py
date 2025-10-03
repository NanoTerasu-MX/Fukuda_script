import os
import subprocess as sp
import logging as log
import time
import pynotify
from pynotify import Event, EventType
import sys 

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


class OpenHandler:
    def handle_event(self, event: Event) -> None:
        # just print out what is happening
        print(f"{event.type.name} at {event.file_path}")

    def can_handle_event_type(self, type: EventType) -> bool:
        return EventType.OPEN & type != 0

class CloseHandler:
    def handle_event(self, event: Event) -> None:
        # just print out what is happening
        print(f"{event.type.name} at {event.file_path}")
        put_file(str(event.file_path))

    def can_handle_event_type(self, type: EventType) -> bool:
        return EventType.CLOSE & type != 0

async def watch_directory(watch_dir):
    with pynotify.Notifier() as notifier:
        notifier.add_watch(watch_dir)

        notifier.modify_watch_event_type(watch_dir, EventType.CLOSE)


def main():
    if len(sys.argv) < 2:
        usage()

    watch_dir = sys.argv[1]

    watch_directory(watch_dir)    

if __name__ == '__main__':
    main()
