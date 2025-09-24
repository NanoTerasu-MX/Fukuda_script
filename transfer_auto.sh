#!/bin/bash


# データの出力パスが書いてあるファイルのディレクトリ
WATCH_DIR="/system/data_transfer"

# 出力パスが書いてあるファイル名
PATTERN="*.txt"

# 転送先
DESTI_DIR="s3://mxdata/mxdata"

# 
AOBA_DIR="/mnt/lustre/S3/a01768/mxdata/mxdata"


LOG=/tmp/transfer.log
touch "$LOG"
# 
log(){
	echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

log "Starting automatically transferring."

# XDS
QSUB=/opt/nec/nqsv/bin/qsub


# データ転送を実行
while true; do
	FILE_LIST=$(find "$WATCH_DIR" -maxdepth 1 -type f -name "$PATTERN")

	# パスが書かれたファイルが存在するか判定
	if [ -n "$FILE_LIST" ]; then
		# 回折データ転送のループ
		for FILE in $FILE_LIST; do
			log "Detect file path: $FILE"
			# 転送するデータのパスの取得 
			DATA_DIR=$(head -n 1 "$FILE")
			TMP_PATH=$(dirname "$DATA_DIR")
			DEST_SUBDIR=$(echo "$TMP_PATH" | sed 's/^\/data//')

			if [ ! -d "$DATA_DIR" ]; then
				log "Warning!: Path is not directory! : $DATA_DIR"
			fi

			# 回折データが存在するか判定
			while true; do
				# 回折データの転送を実行
				if [ "$(ls -A "$DATA_DIR")" ]; then
					log "Data exists. Starting transferring. : $DATA_DIR → $DESTI_DIR$DEST_SUBDIR/"
					s5cmd --profile default --endpoint-url=https://s3ds.cc.tohoku.ac.jp sync "$DATA_DIR" "$DESTI_DIR$DEST_SUBDIR/" >> "$LOG" 2>&1
					log "Complete to transfer of data. : $DATA_DIR"
					mv "$FILE" "$WATCH_DIR/uploaded"

					# run XDS
					DIRNAME=$(dirname "$DATA_DIR")
					SAMPLE_DIR=$(basename "$DATA_DIR")
					sh xds_auto.sh $DIRNAME $SAMPLE_DIR >> "$LOG" 2>&1

					log "Finished SSH execution."

					log "Finished move watch files to uploaded"
					break
				else
					# 回折データがない場合、待機
					log "Data does not yet exist. : $DATA_DIR ; Wait for 10 sec..."
					sleep 10
					continue
				fi
			done
		done
	else
		# パスのファイルがない場合、待機
		log "The file for today's date cannot be found! Wait for 10 sec..."
		sleep 10
		continue
	fi
done

