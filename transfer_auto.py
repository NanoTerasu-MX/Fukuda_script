import os
import subprocess as sp
import logging as log
import time
import sys 
import yaml 

log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

class AutoTransferAndProcess:
    def __init__(self, destination_path_on_s3, destination_path_on_aoba, dataset_path_file, kamo_dataset_path_file):
        self.destination_path_on_S3 = destination_path_on_s3
        self.destination_path_on_aoba = destination_path_on_aoba
        self.dataset_path_file = dataset_path_file
        self.kamo_dataset_path_file = kamo_dataset_path_file
    
    #--- __init__ ---#

    def load_dataset_path_file(self):
        #--- load output path of diffraction data ---#
        with open(self.dataset_path_file, "r") as fin:
            lines = [line.strip() for line in fin if line.strip()]
            if not lines:
                return None, None

        # split the line to each item
        # item[0] = output path
        # item[1] = origin ---> ex. 1
        # item[2] = end value ---> ex. 3600
        line = lines[-1] # latest line
        item = line.split(",")
        transferred_file_path = item[0].strip()
        transferred_file_path = transferred_file_path.removeprefix("/data")
        data_origin, data_total = item[1].strip(), item[2].strip()
        
        log.info(f"transferred file path: {transferred_file_path}")
        log.info(f"Number of data: {data_total}")

        return transferred_file_path, data_total
        
    #--- load_dataset_path_file ---#

    def sync_s3(self):
        while True:
            transferred_file_path, data_total = self.load_dataset_path_file()
            if transferred_file_path is None:
               log.info("No dataset found yet. Waiting...")
               time.sleep(30)
               continue

            dirname = os.path.basename(os.path.dirname(transferred_file_path))
            if dirname.startswith("data"):
                log.info(f"Detected dataset dir: {dirname}")
                self.transfer_and_wait(transferred_file_path, data_total)
                self.write_kamo_dataset_file(transferred_file_path)
                break
            else:
                log.info(f"Non-data directory: {dirname}. Only transferring.")
                self.transfer_and_wait(transferred_file_path, data_total)
                break

    #--- sync_s3 ---#
    
    def transfer_and_wait(self, transferred_file_path):
        #--- iterate transferring, and then the number of files on S3 is consistent with based directory ---#
        while True:
            self.transfer_to_s3(transferred_file_path)

            # count the number of files on S3
            cmd_ls = ["s3cmd", "ls", "-r", self.destination_path_on_s3]
            result = sp.run(cmd_ls, capture_output=True, text=True)
            count = result.stdout.count(".cbf")

            log.info(f"Uploaded {count}/{data_total} images.")
            if data_total and count >= data_total:
                log.info("All files transferred successfully.")
                break
            log.info("Retrying in 60 seconds...")
            time.sleep(60)

    #--- transfer_and_wait ---#

    def transfer_to_s3(self, transferred_file_path):
        destination_path_on_s3 = os.path
        cmd = ["s3cmd", "sync", "--recursive", "--no-check-md5",
               transferred_file_path, self.destination_path_on_s3]
        log.info(f"Running: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        stdout, _ = proc.communicate()
        log.info(stdout)
        log.info(f"Upload finished with returncode {proc.returncode}")

    #--- transfer_to_s3 ---#

    def write_kamo_dataset_file(self):
        transferred_file_path, data_total = self.load_dataset_path_file()
        kamo_proc_path = os.path.join(self.destination_path_on_aoba, transferred_file_path)
        with open(self.kamo_dataset_pathfile, "a") as fout:
                fout.write(f"{kamo_proc_path}\n")
        log.info(f"Wrote path to {self.kamo_dataset_path_file}: {kamo_proc_path}")

    #--- write_kamo_dataset_file ---#

def main():
    #--- load config ---#
    with open("transfer_auto_config.yaml") as fin:
        cfg = yaml.safe_load(f)
        
    auto = AutoTransferAndProcess(
        destination_path_on_s3 = cfg["destination_path_on_s3"],
        destination_path_on_aoba = cfg["destination_path_on_aoba"]
        dataset_path_file = cfg["dataset_path_file"],
        kamo_dataset_pathfile = cfg["kamo_dataset_pathfile"]
        )
    auto.sync_s3()

if __name__ == '__main__':
    main()
