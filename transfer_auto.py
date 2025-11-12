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
    def __init__(self, destination_path_on_s3, destination_path_on_aoba, dataset_path_file, kamo_dataset_path_file, target_date):
        self.destination_path_on_s3 = destination_path_on_s3
        self.destination_path_on_aoba = destination_path_on_aoba
        self.dataset_path_file = dataset_path_file
        self.kamo_dataset_path_file = kamo_dataset_path_file
        self.target_date = target_date
    
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

        dataset_info = []

        for line in lines:
            # loading target date line
            if f"/{self.target_date}_" not in line:
                continue

            item = [s.strip() for s in line.split(",")]
            if len(item) < 3:
                log.error(f"Invalid format in dataset path file: {line}")
                continue
            
            # Remove leading /data if present from transferred_file_path
            transferred_file_path = item[0].strip()
            if transferred_file_path.startswith("/data"):
                transferred_file_path = transferred_file_path[len("/data"):]

            data_origin, data_total = item[1].strip(), item[2].strip()
            dataset_info.append((transferred_file_path, data_origin, data_total))

        if not dataset_info:
            log.info(f"No dataset found for date: {target_date}")
        else:
            log.info(f"Found {len(dataset_info)} datasets for date: {target_date}")

        return dataset_info
        
    #--- load_dataset_path_file ---#

    def sync_s3(self):
        while True:
            dataset_list = self.load_dataset_path_file()
            if not dataset_list:
                log.info("No dataset found yet. Waiting...")
                time.sleep(30)
                continue

            for transferred_file_path, _, _ in dataset_list:
                dirname = os.path.basename(os.path.dirname(transferred_file_path))
                if dirname.startswith("data"):
                    log.info(f"Detected dataset dir: {dirname}")
                    self.transfer_to_s3(transferred_file_path)
                    self.write_kamo_dataset_file()
                    break
                else:
                    log.info(f"Non-data directory: {dirname}. Only transferring.")
                    self.transfer_to_s3(transferred_file_path)
                    break

    #--- sync_s3 ---#
    
    def transfer_to_s3(self, transferred_file_path):
        #--- transfer to S3 ---#
        # obtain full local data directory path
        data_dir = os.path.join("/data", transferred_file_path.lstrip("/"))
        # obtain parent directory
        tmp_path = os.path.dirname(data_dir)
        # remove /data prefix if present
        dest_subdir = os.path.dirname(tmp_path.replace("/data", "", 1) if tmp_path.startswith("/data") else tmp_path)
        # target directory for transfer
        dirname_transferred = tmp_path
        
        log.info(f"data_dir: {data_dir}")
        log.info(f"tmp_path: {tmp_path}")
        log.info(f"dest_subdir: {dest_subdir}")
        log.info(f"dirname_transferred: {dirname_transferred}")

        # Ensure S3 destination path ends with /
        s3_destination = os.path.join(self.destination_path_on_s3, dest_subdir.lstrip("/"))
        cmd = ["s3cmd", "sync", "--recursive", "--no-check-md5",
               dirname_transferred, s3_destination]
        log.info(f"Running: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        stdout, _ = proc.communicate()
        log.info(stdout)
        log.info(f"Upload finished with returncode {proc.returncode}")

    #--- transfer_to_s3 ---#

    def write_kamo_dataset_file(self):
        dataset_list = self.load_dataset_path_file()
        if dataset_list is None:
            log.error("No dataset info to write to kamo_dataset_path_file.")
            return
        
        for transferred_file_path, data_origin, data_total in dataset_list:
            if transferred_file_path.endswith(".h5"):
                transferred_file_path = transferred_file_path[:-3] + ".cbf"
        
            kamo_proc_path = os.path.join(self.destination_path_on_aoba, transferred_file_path.lstrip("/"))
            output_path = f"{kamo_proc_path}, {data_origin}, {data_total}"
        
            try:
              with open(self.kamo_dataset_path_file, "a") as fout:
                 fout.write(f"{output_path}\n")
                log.info(f"Wrote path to {self.kamo_dataset_path_file}: {output_path}")
            except Exception as e:
                log.error(f"Failed to write to kamo_dataset_path_file: {e}")

    #--- write_kamo_dataset_file ---#


def main():
    #--- load arguments ---#
    if len(sys.argv) < 3:
        print("Usage: python script.py kamo_dataset_path_file=<path>, target_data=<YYMMDD>")
        sys.exit(1)

    kamo_dataset_path_file = None
    target_date = None
    for arg in sys.argv[1:]:
        if arg.startswith("kamo_dataset_path_file="):
            kamo_dataset_path_file = arg.split("=", 1)[1]
        elif arg.startswith("target_date="):
            target_date = arg.split("=", 1)[1]

    if kamo_dataset_path_file is None or target_date is None:
        log.info("Error: kamo_dataset_path_file or target_date not specified.")
        log.info("Usage: python script.py kamo_dataset_path_file=<path>, target_date=<YYMMDD>")
        sys.exit(1)

    #--- load config ---#
    with open("transfer_auto_config.yaml") as fin:
        cfg = yaml.safe_load(fin)
        
    auto = AutoTransferAndProcess(
        destination_path_on_s3 = cfg["destination_path_on_s3"],
        destination_path_on_aoba = cfg["destination_path_on_aoba"],
        dataset_path_file = cfg["dataset_path_file"],
        kamo_dataset_path_file=kamo_dataset_path_file,
        target_data=target_date
    )
    auto.sync_s3()

if __name__ == '__main__':
    main()
