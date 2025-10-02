import os
import subprocess as sp
import logging as log
import glob

log.basisConfig(
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
    desti_dir = 's3://mxdata/mxdata'

    log.info('Starting automatically transferring.')
    flag = True
    while flag == True:
        load_dir = os.path.isdir(os.path.join(watch_dir, pattern))
        if os.path.isfile(load_dir):
            log.info(f'{load_dir} is exist')

            for direct in glob.glob(load_dir):
                cmd = f"""s5cmd \
                         --profile default \
                         --endpoint-url=https://s3ds.cc.tohoku.ac.jp \
                         sync \
                         {direct} \
                         {desti_dir}
                       """
                
                p = sp.run(cmd, shell=True, check=True)

                stdout, stderr = p.stdout, p.stderr

                if p.returncode != 0:
                    log.info(f"""{cmd} is abnormal termination\n
                                 returncode={p.returncode}\n
                                 stdout={stdout}\n
                                 stderr={stderr}\n
                              """)

        else:
            continue

def main():
    transfer()


if __name__ == '__main__':
    main()
