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
        self.destination_path_on_s3 = destination_path_on_s3
        self.destination_path_on_aoba = destination_path_on_aoba
        self.dataset_path_file = dataset_path_file
        self.kamo_dataset_path_file = kamo_dataset_path_file
    
    #--- __init__ ---#

    def load_dataset_path_file(self):
        #--- load output path of diffraction data ---#
        try:
            with open(self.dataset_path_file, "r") as fin:
                lines = [line.strip() for line in fin if line.strip()]
        except FileNotFoundError:
            log.error(f"Dataset path file not found: {self.dataset_path_file}")
            return None, None
        
        if not lines:
            log.info("Dataset path file is empty.")
            return None, None

        # split the line to each item
        # item[0] = output path
        # item[1] = origin ---> ex. 1
        # item[2] = end value ---> ex. 3600
        line = lines[-1] # latest line
        item = [s.strip() for s in line.split(",")]
        if len(item) < 3:
            log.error(f"Invalid format in dataset path file: {line}")
            return None, None

        transferred_file_path = item[0].strip()

        # Remove leading /data if present from transferred_file_path
        if transferred_file_path.startswith("/data"):
            transferred_file_path = transferred_file_path[len("/data"):]
        
        data_origin, data_total = item[1].strip(), item[2].strip()
        
        log.info(f"transferred file path: {transferred_file_path}")
        log.info(f"Number of data: {data_total}")

        return transferred_file_path, data_total
        
    #--- load_dataset_path_file ---#

    def sync_s3(self):
        while True:
            transferred_file_path, _ = self.load_dataset_path_file()
            if transferred_file_path is None:
               log.info("No dataset found yet. Waiting...")
               time.sleep(30)
               continue

            dirname = os.path.basename(os.path.dirname(transferred_file_path))
            if dirname.startswith("data"):
                log.info(f"Detected dataset dir: {dirname}")
                self.transfer_to_s3(transferred_file_path)
                self.write_kamo_dataset_file(transferred_file_path)
                break
            else:
                log.info(f"Non-data directory: {dirname}. Only transferring.")
                self.transfer_to_s3(transferred_file_path)
                break

    #--- sync_s3 ---#
    """
    def transfer_and_wait(self, transferred_file_path, data_total):
        #--- iterate transferring, and then the number of files on S3 is consistent with based directory ---#
        expected = None
        if data_total is not None:
            try:
                expected = int(data_total)
            except ValueError:
                log.watrning(f"Invalid data_total value: {data_total}. Proceeding without total count.")
                expected = None

        while True:
            self.transfer_to_s3(transferred_file_path)

            if expected is None:
                log.info("No expected total provided. Assuming transfer is complete.")
                break

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
    """
    def transfer_to_s3(self, transferred_file_path):
        #--- transfer to S3 ---#
        dirname_transferred = os.path.dirname(transferred_file_path)
        cmd = ["s3cmd", "sync", "--recursive", "--no-check-md5",
               dirname_transferred, self.destination_path_on_s3]
        log.info(f"Running: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        stdout, _ = proc.communicate()
        log.info(stdout)
        log.info(f"Upload finished with returncode {proc.returncode}")

    #--- transfer_to_s3 ---#

    def write_kamo_dataset_file(self, transferred_file_path=None):
        if transferred_file_path is None:
            transferred_file_path, _ = self.load_dataset_path_file()
            if transferred_file_path is None:
                log.error("No transferred file path available to write to kamo dataset file.")
                return
        
        kamo_proc_path = os.path.join(self.destination_path_on_aoba, transferred_file_path)
        try:
            with open(self.kamo_dataset_path_file, "a") as fout:
                fout.write(f"{kamo_proc_path}\n")
            log.info(f"Wrote path to {self.kamo_dataset_path_file}: {kamo_proc_path}")
        except Exception as e:
            log.error(f"Failed to write to kamo_dataset_path_file: {e}")

    #--- write_kamo_dataset_file ---#


def main():
    #--- load arguments ---#
    if len(sys.argv) < 2:
        print("Usage: python script.py kamo_dataset_path_file=<path>")
        sys.exit(1)

    kamo_dataset_path_file = None
    for arg in sys.argv[1:]:
        if arg.startswith("kamo_dataset_path_file="):
            kamo_dataset_path_file = arg.split("=", 1)[1]

    if kamo_dataset_path_file is None:
        log.info("Error: kamo_dataset_path_file not specified.")
        log.info("Usage: python script.py kamo_dataset_path_file=<path>")
        sys.exit(1)

    #--- load config ---#
    with open("transfer_auto_config.yaml") as fin:
        cfg = yaml.safe_load(fin)
        
    auto = AutoTransferAndProcess(
        destination_path_on_s3 = cfg["destination_path_on_s3"],
        destination_path_on_aoba = cfg["destination_path_on_aoba"],
        dataset_path_file = cfg["dataset_path_file"],
        kamo_dataset_path_file=kamo_dataset_path_file
    )
    auto.sync_s3()

if __name__ == '__main__':
    main()
