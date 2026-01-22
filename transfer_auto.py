"""
This script automates the transfer of diffraction data files to an S3 bucket and prepares a dataset path file for Kamo processing.
It continuously monitors a specified dataset path file.
"""

# File: transfer_auto.py
# Authors: Akiya Fukuda
# Date: 2025-11-21
# Description: Automated transfer of diffraction data to S3 and preparation of Kamo dataset path file.

#%%
import os
import subprocess as sp
import logging as log
import time
import sys 
import yaml 
import threading
#%%
#--- logging configuration ---#
log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
#--- logging configuration ---#

class AutoTransferAndProcess:
    def __init__(self, 
                 bss_dataset_path: str,
                 destination_path_via_s3: str,
                 destination_path_via_aoba: str,
                 monitor_mode: str,
                 processed_files: set = None):

        # bss_dataset_path: /system/data_transfer/monitor.txt
        # データが測定されると更新されるBSS出力ファイルのパス
        # .dataset_paths_for_kamo.txtの最新のファイルパスが記載されている
        self.bss_dataset_path = bss_dataset_path
        
        # destination_path_via_s3: s3://mxdata/mxdata/
        # s3コマンドを使用してデータを転送する先のS3バケットのパス
        self.destination_path_via_s3 = destination_path_via_s3
        
        # destination_path_via_aoba: /mnt/lustre/S3/a01768/mxdata/mxdata
        # Kamoが参照するデータセットパスファイルを書き込む先のローカルディレクトリのパス
        self.destination_path_via_aoba = destination_path_via_aoba

        # monitoring mode: all or new_only
        # You can choose monitoring mode: all or new_only
        # all: 既にファイルに書かれているパスも含めて全て処理
        # new_only: 新規に追加されたパス（最新行）のみ処理
        
        self.mode = monitor_mode

        self.processed_files = set()  # To keep track of already processed file paths

    #--- __init__ ---#

    # updated path 2025-11-26 by Akiya Fukuda
    def path(self):
        #--- load diffraction data path via BSS output file ---#
        # self.bss_dataset_path: /system/data_transfer/monitor.txt
        try:
            with open(self.bss_dataset_path, "r") as fin:
                # output_path_by_bss:
                # /data/mxstaff/.dataset_paths_for_kamo.txt
                # or
                # /data/<Beamtime ID>/Data/.dataset_paths_for_kamo.txt 

                """
                readlines()は読み込んだファイルのテキストを行ごとにリスト化する.
                例:
                <読み込むファイルの中身>
                line1
                line2
                line3

                これをreadlines()で読み込むと
                ['list1', 'list2', 'list3']
                とリストが返ってくる.
                したがって、最新のファイルパスを取得したい場合, リストの一番最後である[-1]を持ってくれば良い
                """

                lines = fin.readlines()

                if not lines:
                    log.info(f"Dataset path file is empty: {self.bss_dataset_path}")
                    return None

                if self.mode == "new_only":
                    output_path_by_bss = lines[-1].strip()
                elif self.mode == "all":
                    output_path_by_bss = "".join(lines).strip()

                if not output_path_by_bss:
                    log.info(f"The last line of the dataset path file is empty: {self.bss_dataset_path}")
                    return None

                log.info(f"output dataset to {output_path_by_bss}")
                return output_path_by_bss
            
        except FileNotFoundError:
            log.error(f"Dataset path not found: {self.bss_dataset_path}")
            return None
        
    #--- path ---#

    # updated identify_auto_or_visit 2025-11-21 by Akiya Fukuda
    def identify_auto_or_visit(self, 
                               output_path_by_bss: str):
        #--- identify auto or visit mesurement ---#
        """
        output_path_by_bss by self.load_dataset_path_file() is identified as 
        /data/mxstaff/.dataset_paths_for_kamo.txt (auto measurement)
        or 
        /data/<Beamline ID>/.dataset_paths_for_kamo.txt (visit measurement)
        """

        if "/data/mxstaff/" in output_path_by_bss:
            log.info("Auto measurement detected. ---> Return auto")
            return "auto"
        else:
            log.info("Visit measurement detected. ---> Return visit")
            return "visit"

    #--- identify_auto_or_visit ---#

    # updated load_dataset_paths_for_kamo_file 2025-12-16 by Akiya Fukuda
    def load_dataset_paths_for_kamo_file(self,
                                         output_path_by_bss: str):
        """
        Reads the LAST LINE of a .dataset_paths_for_kamo.txt file 
        and parses an entry of the form:
        /some/path, 1, 123

        Returns a dict like:
        {"path": "/some/path", "total": 123}

        Returns None on I/O error or parsing failure.
        """
        
        try:
            with open(output_path_by_bss, "r") as fin:
                lines = fin.readlines()

                if not lines:
                    log.error(f"File is empty:{output_path_by_bss}")
                    return None

                latest_line = lines[-1].strip()

                if not latest_line:
                    log.error(f"The last line of the dataset path file is empty: {latest_line}")
                    return None

                try:
                    path_str, data_origin_str, total_str = latest_line.split(",", 2)
                except ValueError:
                    log.warning(f"Could not parse the line. Incorrect format: '{latest_line}'")
                    return None

                dataset_path = path_str.strip()
                data_origin = int(data_origin_str.strip())
                total = int(total_str.strip())

                if os.path.basename(dataset_path) in ".h5":
                    dataset_path = dataset_path[:-3] + ".cbf"                

                log.info(f"Successfully parsed: path='{dataset_path}', data_origin={data_origin}total={total}")
                return {"path": dataset_path, "data_origin": data_origin, "total": total}            

        except FileNotFoundError:
            log.error(f"{output_path_by_bss} is not exist")
            return None
        
        except Exception as e:
            log.error(f"An unexpected error occurred: {e}")
            return None

    #--- load_dataset_paths_for_kamo_file ---#


    # updated identify_data_or_other 2025-11-21 by Akiya Fukuda
    def identify_data_or_other(self, dataset_path: str):
        #--- identify data or other files ---#
        # dataset_path by self.load_dataset_paths_for_kamo() is identified as 
        # /data/.../data/
        # or 
        # /data/.../<other>/ (e.g., scan, check etc.)

        # basename:
        # data
        # or
        # <other> (e.g., scan, check etc.)

        # パスがファイル名（例: *.cbf）を含む場合、ディレクトリ名を取得するためにos.path.dirnameを使用
        if os.path.isfile(dataset_path) or "*" in dataset_path:
            # 例: /data/.../data01/*.cbf -> /data/.../data01
            path_to_check = os.path.dirname(dataset_path)
        else:
            # 例: /data/.../data/
            path_to_check = dataset_path

        basename = os.path.basename(path_to_check.rstrip("/"))

        if "data" == basename:
            log.info(f"{basename} detected.")
            return "data"
        else:
            log.info(f"{basename} detected.")
            return "other"
    
    #--- identify_data_or_other ---#

    # updated proc 2025-11-21 by Akiya Fukuda
    ''' main process '''
    def proc(self):
        while True:
            '''
            メインのループ処理
            1. self.path()で/system/data_transfer/monitor.txtから.dataset_paths_for_kamo.txt(=output_path_by_bss)の最新のファイルパスを取得
            2. self.identify_auto_or_visit()で, output_path_by_bssからauto測定かvisit測定かを判別
               auto測定の場合: /data/mxstaff/.dataset_paths_for_kamo.txt
                             データ転送とkamoによる処理を行う
               visit測定の場合: /data/<Beamline ID>/.dataset_paths_for_kamo.txt
                             データ転送のみを行う
            3. self.load_dataset_paths_for_kamo_file()でkamoが処理すべき最新のデータセットパスと総フレーム数を取得
               dataset_path: /data/.../Data/YYMMDD_BL09U/../data or /(other)
            4. self.identify_data_or_other()でdataディレクトリかその他のディレクトリかを判別
               もしdataディレクトリであれば、データ転送とKamo用のデータセットパスファイルへの書き込みを行う
               もしその他のディレクトリであれば、データ転送のみを行う
            5. self.transfer_to_s3()でS3へデータを転送
            6. self.write_kamo_dataset_file()でKamo用のデータセットパスファイルに書き込み
            7. 処理済みファイルパスを保存して重複処理を防止
            8. 一定時間待機してループを繰り返す
            以上の処理を無限ループで繰り返す
            これにより、新しいデータセットが追加されるたびに自動的に処理が行われる
            30秒ごとに最新のデータセットパスをチェックする
            もし新しいデータセットが見つかれば、それをS3に転送し、Kamo用のデータセットパスファイルに追加する
            これにより、データの転送とKamo処理の準備が自動化される
            '''
            output_path_by_bss = self.path()
            if not output_path_by_bss:
                log.info("No output_path_by_bss found yet. Waiting...")
                time.sleep(30)
                continue

            if dataset_path in self.processed_files:
                log.info(f"Already processed: {dataset_path}. Waiting for new data...")
                time.sleep(30)
                continue

            if "auto" == self.identify_auto_or_visit(output_path_by_bss):
                log.info("Detected auto measurement.")

                dataset_info = self.load_dataset_paths_for_kamo_file(output_path_by_bss)
                if dataset_info is None:
                    log.error("Failed to load dataset info.")
                    continue

                dataset_path = dataset_info["path"]
                total = dataset_info["total"]

                if "data" == self.identify_data_or_other(dataset_path):
                    log.info("Detected data directory. Transferring and preparing Kamo dataset file.")
                    self.transfer_to_s3(dataset_path)
                    self.write_kamo_dataset_file(dataset_path, data_origin=1, data_total=total)

                elif "other" == self.identify_data_or_other(dataset_path):
                    log.info("Non-data directory detected. Only transferring.")
                    self.transfer_to_s3(dataset_path)

            elif "visit" == self.identify_auto_or_visit(output_path_by_bss):
                log.info("Detected visit measurement.")

                dataset_info = self.load_dataset_paths_for_kamo_file(output_path_by_bss)
                if dataset_info is None:
                    log.error("Failed to load dataset info.")
                    continue

                dataset_path = dataset_info["path"]
                total = dataset_info["total"]

                if "data" == self.identify_data_or_other(dataset_path):
                    log.info("Detected data directory. Transferring and preparing Kamo dataset file.")
                    self.transfer_to_s3(dataset_path)
                
                elif "other" == self.identify_data_or_other(dataset_path):
                    log.info("Non-data directory detected. Only transferring.")
                    self.transfer_to_s3(dataset_path)
            
            # Save processed file path
            self.processed_files.add(dataset_path)


    #--- proc ---#

    #--- updated transfer_to_s3 2025-12-16 by Akiya Fukuda ---#
    def transfer_to_s3(self, dataset_path: str):
        #--- transfer to S3 ---#
        # obtain full local data directory path
        data_dir = dataset_path.rstrip("/")
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
        s3_destination = os.path.join(self.destination_path_via_s3, dest_subdir.lstrip("/"))
        if not s3_destination.endswith("/"):
            s3_destination += "/"
            
        log.info(f"s3_destination: {s3_destination}")
        cmd = ["s3cmd", "sync", "--recursive", "--no-check-md5",
               dirname_transferred, s3_destination]
        log.info(f"Running: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        stdout, _ = proc.communicate()
        log.info(stdout)
        log.info(f"Upload finished with returncode {proc.returncode}")

    #--- transfer_to_s3 ---#

    def write_kamo_dataset_file(self, dataset_path: str, data_origin: int = 1, data_total: int = None):
        if dataset_path is None:
            log.error(f"No dataset info to write to {dataset_path}")
            return

        kamo_proc_path = os.path.join(self.destination_path_via_aoba, dataset_path.lstrip("/"))
        output_path = f"{kamo_proc_path}, {data_origin}, {data_total}"
        
        try:
            with open(kamo_proc_path, "a") as fout:
                fout.write(f"{output_path}\n")
                log.info(f"Wrote path to {kamo_proc_path}: {output_path}")
        except Exception as e:
            log.error(f"Failed to write to {kamo_proc_path}: {e}")

    #--- write_kamo_dataset_file ---#

#%%
def main():
    
    #--- load config ---#
    with open("transfer_auto_config.yaml") as fin:
        cfg = yaml.safe_load(fin)
        
    auto = AutoTransferAndProcess(
        bss_dataset_path=cfg["bss_dataset_path"],
        destination_path_via_s3=cfg["destination_path_via_s3"],
        destination_path_via_aoba=cfg["destination_path_via_aoba"],
        monitor_mode=cfg["monitor_mode"]
    )
    auto.proc()
#%%
if __name__ == '__main__':
    main()
