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
import re  # added 2026-05-14 by Akiya Fukuda
import subprocess as sp
import logging as log
import time
import sys
import yaml
import threading
from pathlib import Path

# -*- coding: utf-8 -*-

#--- logging configuration ---#
log.basicConfig(
    filename='transfer.log',
    level=log.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
#--- logging configuration ---#

class AutoTransferAndProcess:
    def __init__(self, cfg):

        # bss_dataset_path: /system/data_transfer/monitor.txt
        # データが測定されると更新されるBSS出力ファイルのパス
        # .dataset_paths_for_kamo.txtの最新のファイルパスが記載されている
        self.bss_dataset_path = cfg["bss_dataset_path"]

        # destination_path_via_s3: s3://mxdata/mxdata/
        # s3コマンドを使用してデータを転送する先のS3バケットのパス
        self.destination_path_via_s3 = cfg["destination_path_via_s3"]

        # destination_path_via_aoba: /mnt/lustre/S3/a01768/mxdata/mxdata
        # Kamoが参照するデータセットパスファイルを書き込む先のローカルディレクトリのパス
        self.destination_path_via_aoba = cfg["destination_path_via_aoba"]

        # monitoring mode: all or new_only
        self.monitor_mode = cfg["monitor_mode"]

        # dataset mode: all or new_only
        self.dataset_mode = cfg["dataset_mode"]

        # wait time between checks (in seconds)
        self.wait_time = cfg["wait_time"]

        # number of threads for parallel transfer
        self.num_threads = cfg["num_threads"]

        # To keep track of already processed file paths
        self.processed_files = set()

        # added 2026-05-14 by Akiya Fukuda: Lock for thread-safe writes to dataset_paths_for_kamo.txt
        self._kamo_file_lock = threading.Lock()

    #--- __init__ ---#

    # updated path 2025-11-26 by Akiya Fukuda
    def path(self):
        #--- load diffraction data path via BSS output file ---#
        # self.bss_dataset_path: /system/data_transfer/monitor.txt
        try:
            with open(self.bss_dataset_path, "r") as fin:
                lines = fin.readlines()

                if not lines:
                    log.info(f"Dataset path file is empty: {self.bss_dataset_path}")
                    return None

                if self.monitor_mode == "new_only":
                    output_path_by_bss = lines[-1].strip()
                elif self.monitor_mode == "all":
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
                lines = [l.strip() for l in fin if l.strip()]

                if not lines:
                    log.error(f"File is empty:{output_path_by_bss}")
                    return []

                target_lines = lines if self.dataset_mode == "all" else [lines[-1]]

                if not target_lines:
                    log.error(f"The last line of the dataset path file is empty: {target_lines}")
                    return None

                parsed_results = []
                for line in target_lines:
                    try:
                        path_str, data_origin_str, total_str = line.split(",", 2)
                        dataset_path = path_str.strip()
                        if dataset_path.endswith(".h5"):
                            dataset_path = dataset_path[:-3] + ".cbf"

                        parsed_results.append({
                            "path": dataset_path,
                            "data_origin": int(data_origin_str.strip()),
                            "total": int(total_str.strip())
                        })
                    except ValueError:
                        log.warning(f"Skipping invalid line: {line}")
                        continue

                return parsed_results

        except FileNotFoundError:
            log.error(f"{output_path_by_bss} is not exist")
            return []

        except Exception as e:
            log.error(f"An unexpected error occurred: {e}")
            return None

    #--- load_dataset_paths_for_kamo_file ---#


    # updated identify_data_or_other 2025-11-21 by Akiya Fukuda
    def identify_data_or_other(self, dataset_path: str):
        if os.path.isfile(dataset_path) or "*" in dataset_path or "?" in dataset_path:
            path_to_check = os.path.dirname(dataset_path)
        else:
            path_to_check = dataset_path

        basename = os.path.basename(path_to_check.rstrip("/"))

        if "data" in basename:
            log.info(f"{basename} detected.")
            return "data"
        else:
            log.info(f"{basename} detected.")
            return "other"

    #--- identify_data_or_other ---#

    # added 2026-05-14 by Akiya Fukuda: extract CBF filename prefix for narrow sync targeting
    def _extract_cbf_prefix(self, dataset_path: str):
        """Extract the filename prefix before wildcard characters (?, *) for targeted sync.

        Example: 'CPS7135-02-multi_001_??????.cbf' -> 'CPS7135-02-multi_001_'
        Returns None if no wildcard found (helical / non-multi case).
        """
        fname = Path(dataset_path).name
        match = re.match(r'^([^?*]+)[?*]', fname)
        if match:
            prefix = match.group(1)
            log.info(f"[_extract_cbf_prefix] Extracted prefix '{prefix}' from '{fname}'")
            return prefix
        log.info(f"[_extract_cbf_prefix] No wildcard in '{fname}'; using broad sync.")
        return None

    #--- _extract_cbf_prefix ---#

    # updated proc 2025-11-21 by Akiya Fukuda
    # updated proc 2026-01-23 by Akiya Fukuda
    # updated proc 2026-05-14 by Akiya Fukuda (threaded sync + kamo)
    ''' main process '''
    def proc(self):
        while True:
            output_path_by_bss = self.path()
            if not output_path_by_bss:
                log.info("No output_path_by_bss found yet. Waiting...")
                time.sleep(self.wait_time)
                continue

            dataset_info = self.load_dataset_paths_for_kamo_file(output_path_by_bss)
            if dataset_info is None:
                log.error("Failed to load dataset info.")
                time.sleep(self.wait_time)
                continue

            # Obtain the index of the latest line
            last_index = len(dataset_info) - 1

            for i, info in enumerate(dataset_info):
                dataset_path = info["path"]
                total = info["total"]

                if i < last_index and dataset_path in self.processed_files:
                    log.info(f"Dataset path already processed: {dataset_path}. Skipping...")
                    continue
                else:
                    log.info(f"Syncing dataset: {dataset_path} (Latest: {i == last_index})")

                if dataset_path not in self.processed_files:
                    log.info(f"Processing new dataset path: {dataset_path}")

                    # Extract prefix for multi-measurement narrowing (e.g. "CPS7135-02-multi_001_")
                    cbf_prefix = self._extract_cbf_prefix(dataset_path)

                    measurement_type = self.identify_auto_or_visit(output_path_by_bss)
                    dir_type = self.identify_data_or_other(dataset_path)

                    if dir_type == "data":
                        log.info(f"[{measurement_type}] Data directory detected. "
                                 f"Starting parallel sync + kamo threads "
                                 f"(prefix={cbf_prefix!r}).")

                        # Thread A: data transfer (narrowed by prefix for multi-measurement)
                        t_sync = threading.Thread(
                            target=self._run_transfer_to_s3,
                            args=(dataset_path, cbf_prefix),
                            name=f"sync-{Path(dataset_path).name[:40]}",
                            daemon=True,
                        )
                        # Thread B: write dataset_paths_for_kamo.txt and upload to S3
                        t_kamo = threading.Thread(
                            target=self._run_write_kamo,
                            args=(dataset_path, 1, total),
                            name=f"kamo-{Path(dataset_path).name[:40]}",
                            daemon=True,
                        )
                        t_sync.start()
                        t_kamo.start()
                        log.info(f"Threads started: '{t_sync.name}', '{t_kamo.name}'")

                    elif dir_type == "other":
                        log.info(f"[{measurement_type}] Non-data directory detected. "
                                 f"Starting sync thread only.")
                        t_sync = threading.Thread(
                            target=self._run_transfer_to_s3,
                            args=(dataset_path, None),
                            name=f"sync-other-{Path(dataset_path).name[:30]}",
                            daemon=True,
                        )
                        t_sync.start()
                        log.info(f"Thread started: '{t_sync.name}'")

            if len(dataset_info) > 1:
                for info in dataset_info[:-1]:
                    self.processed_files.add(info["path"])

            log.info("Sync cycle finished. Waiting...")
            time.sleep(self.wait_time)

    #--- proc ---#

    # added 2026-05-14 by Akiya Fukuda: thread wrapper for transfer_to_s3 (Thread A — data sync)
    def _run_transfer_to_s3(self, dataset_path: str, cbf_prefix: str):
        """Thread wrapper for transfer_to_s3 — catches and logs any uncaught exception."""
        try:
            self.transfer_to_s3(dataset_path, cbf_prefix=cbf_prefix)
        except Exception as e:
            log.error(f"[sync thread] Uncaught exception for '{dataset_path}': {e}", exc_info=True)

    # added 2026-05-14 by Akiya Fukuda: thread wrapper for write_kamo_dataset_file (Thread B — kamo file)
    def _run_write_kamo(self, dataset_path: str, data_origin: int, data_total: int):
        """Thread wrapper for write_kamo_dataset_file — catches and logs any uncaught exception."""
        try:
            self.write_kamo_dataset_file(dataset_path, data_origin=data_origin, data_total=data_total)
        except Exception as e:
            log.error(f"[kamo thread] Uncaught exception for '{dataset_path}': {e}", exc_info=True)

    #--- updated transfer_to_s3 2025-12-16 by Akiya Fukuda ---#
    #--- updated transfer_to_s3 2026-02-26 by Akiya Fukuda ---#
    #--- updated transfer_to_s3 2026-05-14 by Akiya Fukuda: added cbf_prefix parameter; narrow sync (find | xargs s3cmd put) when prefix present, broad sync (original) otherwise ---#
    def transfer_to_s3(self, dataset_path: str, cbf_prefix: str = None):
        #--- transfer to S3 ---#
        # obtain full local data directory path
        if os.path.isfile(dataset_path) or "*" in dataset_path or "?" in dataset_path:
            data_dir = os.path.dirname(dataset_path)
        else:
            data_dir = dataset_path.rstrip("/")

        if cbf_prefix:
            # ----------------------------------------------------------------
            # Narrow sync: upload only files whose name starts with cbf_prefix
            # in the immediate data directory (e.g. data02/).
            # This prevents multi_002 / multi_003 ... from being included when
            # we are processing multi_001.
            # ----------------------------------------------------------------

            # Map local data_dir to its S3 equivalent by stripping the leading /data
            if data_dir.startswith("/data/"):
                relative_data = data_dir[len("/data/"):]
            elif data_dir.startswith("/data"):
                relative_data = data_dir[len("/data"):]
            else:
                relative_data = data_dir.lstrip("/")

            s3_data_dir = self.destination_path_via_s3.rstrip("/") + "/" + relative_data + "/"

            log.info(f"[transfer_to_s3] Narrow sync — prefix='{cbf_prefix}'")
            log.info(f"[transfer_to_s3] data_dir: {data_dir}")
            log.info(f"[transfer_to_s3] s3_data_dir: {s3_data_dir}")

            # find matching files and upload each one in parallel
            cmd = (
                f"find '{data_dir}' -maxdepth 1 -name '{cbf_prefix}*.cbf' -type f | "
                f"xargs -P {self.num_threads} -I{{}} s3cmd put --no-check-md5 {{}} '{s3_data_dir}'"
            )
        else:
            # ----------------------------------------------------------------
            # Broad sync: 2 levels above the data directory (original logic).
            # Used for helical or any non-wildcard path.
            # ----------------------------------------------------------------
            parts = Path(data_dir).parts
            data_idx = None
            for i, part in enumerate(parts):
                if "data" in part:
                    data_idx = i
            if data_idx is not None and data_idx >= 2:
                dirname_transferred = str(Path(*parts[:data_idx - 1]))
            else:
                dirname_transferred = str(Path(data_dir).parent)

            # remove /data prefix if present, then go one more level up for the S3 root
            dest_subdir = os.path.dirname(
                dirname_transferred.replace("/data", "", 1)
                if dirname_transferred.startswith("/data")
                else dirname_transferred
            )

            log.info(f"[transfer_to_s3] Broad sync")
            log.info(f"[transfer_to_s3] data_dir: {data_dir}")
            log.info(f"[transfer_to_s3] dirname_transferred: {dirname_transferred}")
            log.info(f"[transfer_to_s3] dest_subdir: {dest_subdir}")

            s3_destination = os.path.join(self.destination_path_via_s3, dest_subdir.lstrip("/"))
            if not s3_destination.endswith("/"):
                s3_destination += "/"

            log.info(f"[transfer_to_s3] Target: {dirname_transferred} -> {s3_destination}")

            cmd = (
                f"s3cmd sync --dry-run --no-check-md5 '{dirname_transferred}' '{s3_destination}' | "
                f"grep 'upload:' | "
                f"sed -E \"s/upload: '([^']*)' -> '([^']*)'.*/\\1 \\2/\" | "
                f"xargs -n 2 -P {self.num_threads} s3cmd put --no-check-md5"
            )

        log.info(f"[transfer_to_s3] Executing parallel upload with {self.num_threads} threads...")
        log.info(f"[transfer_to_s3] Command: {cmd}")
        try:
            proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
            stdout, _ = proc.communicate()

            if stdout:
                log.info(f"[transfer_to_s3] Output:\n{stdout}")

            if proc.returncode == 0:
                log.info(f"[transfer_to_s3] Upload finished successfully.")
            else:
                log.error(f"[transfer_to_s3] Upload failed with returncode {proc.returncode}")

        except Exception as e:
            log.error(f"[transfer_to_s3] Error during parallel transfer: {e}")

    #--- transfer_to_s3 ---#

    #--- updated write_kamo_dataset_file 2026-05-14 by Akiya Fukuda: added _kamo_file_lock for thread-safe concurrent writes from Thread B ---#
    def write_kamo_dataset_file(self, dataset_path: str, data_origin: int = 1, data_total: int = None):
        if dataset_path is None:
            log.error(f"No dataset info to write to {dataset_path}")
            return

        p = Path(dataset_path)

        # --- ファイル名を除外してディレクトリのパーツのみを取得 ---
        if os.path.isfile(dataset_path) or "*" in dataset_path or "?" in dataset_path or dataset_path.endswith((".cbf", ".h5")):
            dir_parts = p.parent.parts
        else:
            dir_parts = p.parts

        # Find the deepest data directory (data, data00, data01, ...) and use its parent
        data_idx = None
        for i, part in enumerate(dir_parts):
            if "data" in part:
                data_idx = i

        if data_idx is not None and data_idx >= 1:
            # Parent of the CPS directory (same level as CPS*)
            base_parent = Path(*dir_parts[:data_idx - 1])
        else:
            base_parent = p.parents[2]
        # ------------------------------------------------------------------

        dest_subdir = base_parent.relative_to("/data")
        write_kamo_proc_path = os.path.join(self.destination_path_via_s3, str(dest_subdir))
        if not write_kamo_proc_path.endswith('/'):
           write_kamo_proc_path += '/'

        # S3-side path: /data/mxstaff/Data/... -> /mnt/lustre/S3/a01768/mxdata/mxdata/mxstaff/Data/...
        output_path = p.relative_to("/data")
        output_path = os.path.join(self.destination_path_via_aoba, str(output_path))
        output_sets = f"{output_path}, {data_origin}, {data_total}"

        local_write_kamo_proc_path = os.path.join(str(base_parent), "dataset_paths_for_kamo.txt")

        log.info(f"[write_kamo] dataset_path: {dataset_path}")
        log.info(f"[write_kamo] base_parent: {base_parent}")
        log.info(f"[write_kamo] dest_subdir: {dest_subdir}")
        log.info(f"[write_kamo] write_kamo_proc_path (S3): {write_kamo_proc_path}")
        log.info(f"[write_kamo] local_kamo_proc_path: {local_write_kamo_proc_path}")
        log.info(f"[write_kamo] output line to write: {output_sets}")

        try:
            # Lock so concurrent kamo threads don't interleave file writes or S3 puts
            with self._kamo_file_lock:
                existing_content = ""
                if os.path.isfile(local_write_kamo_proc_path):
                    with open(local_write_kamo_proc_path, "r") as f:
                        existing_content = f.read()

                if f"{output_sets}\n" in existing_content:
                    log.info(f"[write_kamo] Path already exists in {local_write_kamo_proc_path}. Skipping.")
                    return

                with open(local_write_kamo_proc_path, "a") as fout:
                    fout.write(f"{output_sets}\n")
                log.info(f"[write_kamo] Wrote to {local_write_kamo_proc_path}: {output_sets}")

                log.info(f"[write_kamo] Uploading {local_write_kamo_proc_path} -> {write_kamo_proc_path}")
                cmd = f"s3cmd put '{local_write_kamo_proc_path}' '{write_kamo_proc_path}'"
                log.info(f"[write_kamo] Command: {cmd}")
                sp.run(cmd, shell=True, check=True)
                log.info(f"[write_kamo] dataset_paths_for_kamo.txt transferred successfully.")

        except ValueError as e:
            log.error(f"[write_kamo] Failed to write to {local_write_kamo_proc_path}: {e}")
        except Exception as e:
            log.error(f"[write_kamo] Unexpected error: {e}", exc_info=True)

    #--- write_kamo_dataset_file ---#

#%%
def main():

    #--- load config ---#
    with open("transfer_auto_config.yaml") as fin:
        cfg = yaml.safe_load(fin)

    auto = AutoTransferAndProcess(cfg=cfg)
    auto.proc()
#%%
if __name__ == '__main__':
    main()
