#!/bin/bash

QSUB=/opt/nec/nqsv/bin/qsub

# Get the date
LOG=/tmp/transfer.log
log(){
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

log "Get argument"
AOBA_DIR="/mnt/lustre/S3/a01768/mxdata/mxdata"
ARG1=$(echo "$1")
CROP_ARG1=$(echo "$ARG1" | sed 's/^\/data//')
SAMPLE_DIR="$AOBA_DIR/$CROP_ARG1"
DATA_DIR="$SAMPLE_DIR/$(echo "$2")"
XDS_DIR="$DATA_DIR/XDS"
FILE_NAME="$2_00????.cbf"

log "XDS_DIR: $XDS_DIR"
log "DATA_DIR/FILE_NAME: $DATA_DIR/$FILE_NAME"

# Make job file for AOBA-S
log "Step1: Connection to AOBA-S for making job file"
ssh sfront <<EOFSSH
mkdir -p  "$XDS_DIR"
cat <<'EOF' > xds.sh
#!/bin/sh
#PBS -q sxs
#PBS --venode 1
#PBS -l elapstim_req=2:00:00
cd \$PBS_O_WORKDIR
xds_par
EOF
mv "xds.sh" "$XDS_DIR"
EOFSSH

# Connection to AOBA-S and execution
log "Step2: Connection to AOBA-S"
ssh sfront <<EOF
cd "$XDS_DIR"
generate_XDS.INP "$DATA_DIR/$FILE_NAME"
$QSUB xds.sh
EOF

log "Finished SSH execution."
