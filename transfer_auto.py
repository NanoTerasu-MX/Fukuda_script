import os
import subprocess as sp
import logging as log
import glob
import time

log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

def usage():
    print('Usage: python script.py <watch_dir>')
    sys.exit(1)

def transfer(watch_dir):
    
    # destination directory
    desti_dir = 's3://mxdata/mxdata/mxstaff/Data/'
    
    log.info('--------------------------------------------------')
    log.info('--------------------------------------------------')
    log.info('---Welcome to automatically transferring system---')
    log.info('--------------------------------------------------')
    log.info('--------------------------------------------------')
    
    flag = True
    while flag == True:
        if os.path.isdir(watch_dir):
            cmd = f"""time \
                      /data/mxstaff/s3command/bins/s5cmd \
                      --profile default \
                      --endpoint-url=https://s3ds.cc.tohoku.ac.jp \
                      sync \
                      {watch_dir} \
                      {desti_dir}
                   """
            
            try:
                log.info(f"""Starting Transferring\n
                             {watch_dir} ---> {desti_dir}\n""")

                with sp.Popen(cmd, shell=True, text=True, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
                   for line in proc.stdout:
                       log.info(line.strip())

                   for line in proc.stderr:
                       log.info(line.strip())   

                   proc.wait()
                   log.info(f"""Transferring is normal termination\n
                                {watch_dir} ---> {desti_dir}\n""")

            except sp.CalledProcessError as e:
                log.error(f"""Error transferring {direct}\n
                              Return code: {e.returncode}\n
                              stdout={e.stdout}\n
                              stderr={e.stderr}""")

        time.sleep(10)

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    watch_dir = sys.argv[1]
    
    transfer(watch_dir)

if __name__ == '__main__':
    main()
