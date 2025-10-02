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

def transfer():
    
    # watch directory path
    watch_dir = '/data/mxstaff/Data'
    
    # synchronizes directory pattern  
    pattern = '*_BL09U'
    
    # destination directory
    desti_dir = 's3://mxdata/mxdata/mxstaff/Data'

    log.info('Starting automatically transferring.')
    flag = True
    while flag == True:
        load_dirs = glob.glob(os.path.join(watch_dir, pattern))
            
            if load_dirs:
                for direct in load_dirs:
                    if os.path.isdir(direct):
                        cmd = f"""time \
                                 /data/mxstaff/s3command/bins/s5cmd \
                                 --profile default \
                                 --endpoint-url=https://s3ds.cc.tohoku.ac.jp \
                                 sync \
                                 {direct} \
                                 {desti_dir}
                               """
                   
                        try: 
                            p = sp.run(cmd, 
                                       shell=True,
                                       text=True,
                                       check=True,
                                       capture_output=True)
                        
                            log.info(f'Transfer finished: {direct}\n
                                       stdout={p.stdout}\n
                                       stderr={p.stderr}')

                        except sp.CalledProcessError as e:
                             log.error(f'Error transferring {direct}\n
                                         Return code: {e.returncode}\n
                                         stdout={e.stdout}\n
                                         stderr={e.stderr}')

            time.sleep(10)

def main():
    transfer()


if __name__ == '__main__':
    main()
