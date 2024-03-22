#!/bin/bash

# If an argument for a storage account URL is provided we'll upload the archive to the storage account:
if [ -n "$1" ]; then
    # Remove trailing '/' if present:
    export STORAGE_ACCOUNT_URL="${1%/}"
    echo "Will upload archive to $STORAGE_ACCOUNT_URL"
fi

# Create the archive for TES Performace monitoring:
if [ -z ${TELEGRAD_BUILD_DIR+x} ]; then
    export TELEGRAD_BUILD_DIR="$HOME/telegraf"
fi
if [ -z ${AGENT_PERF_SCRIPT_DIR+x} ]; then
    export AGENT_PERF_SCRIPT_DIR="$PWD/vm_monitor_scripts"
fi
ARCHIVE_NAME="tes_vm_monitor"
export TELEGRAF_BUILD_FILENAME=""

function build_telegraf() {
    local return_pwd
    return_pwd=$(pwd)

    # Build telegraf from source:
    cd "$TELEGRAD_BUILD_DIR" || exit 1
    rm -f ./telegraf
    make build_tools
    export LDFLAGS="-s -w"
    if ! ./tools/custom_builder/custom_builder -tags -config "${AGENT_PERF_SCRIPT_DIR}/tes_vm_monitor.once.conf" -config "${AGENT_PERF_SCRIPT_DIR}/tes_vm_monitor.continuous.conf" -config "${AGENT_PERF_SCRIPT_DIR}/tes_vm_monitor.dummy.conf"; then
        echo "Error: Failed to run custom builder"
        exit 1
    fi
    # Make sure a telegraf binary was built:
    if [ ! -f ./telegraf ]; then
        echo "Error: telegraf binary not found at $TELEGRAD_BUILD_DIR/telegraf"
        exit 1
    fi
    # Print stats on telegraf binary size:
    ls -lh ./telegraf
    # Print stats on compressed size vs built size:
    GZIP_SZ=$(gzip --best -c ./telegraf | wc -c)
    SIZE=$(stat -c %s ./telegraf)
    GZIP_SZ_MB=$(echo "scale=2; $GZIP_SZ / 1024 / 1024" | bc)
    SIZE_MB=$(echo "scale=2; $SIZE / 1024 / 1024" | bc)
    REDUCTION=$(echo "scale=2; (1 - $GZIP_SZ / $SIZE) * 100" | bc)
    echo "telegraf binary size: $SIZE_MB MB, compressed: $GZIP_SZ_MB MB, size reduction: $REDUCTION%"
    TELEGRAF_BUILD_FILENAME=$(find "${TELEGRAD_BUILD_DIR}" -maxdepth 1 -type f -name "telegraf" -print0)
    cd "$return_pwd" || exit 1
}

function build_telegraf_remote(){
    local return_pwd
    return_pwd=$(pwd)

    cd ./telegraf || exit 1
    ./build_telegraf_remote.sh
    TELEGRAF_BUILD_FILENAME="$PWD/telegraf"
    cd "$return_pwd" || exit 1
}

# Create a tar archive of both the telegraf binary and the tes scripts:
echo "$TELEGRAD_BUILD_DIR"
build_telegraf_remote

# Use find to gather the files, and then tar them into an archive. Transform
# the paths to remove the leading directories.
rm -f "${ARCHIVE_NAME}.tar"
rm -f "${ARCHIVE_NAME}.tar.gz"
( printf "%s\0" "$TELEGRAF_BUILD_FILENAME"  ; find "${AGENT_PERF_SCRIPT_DIR}" -type f -print0 ) | tar --null -cvf "${ARCHIVE_NAME}.tar" --transform 's,^.*/,,S' -T -
# Compress using gzip for size and decompression speed:
gzip --best "${ARCHIVE_NAME}.tar"

# Print stats on archive size:
SIZE=$(stat -c %s "${ARCHIVE_NAME}.tar.gz")
SIZE_MB=$(echo "scale=2; $SIZE / 1024 / 1024" | bc)
echo "Total archive size: $SIZE_MB MB"

# Print the contents of the archive:
echo -e "\nArchive contents:"
tar -tvf "${ARCHIVE_NAME}.tar.gz"
echo -e "\n${ARCHIVE_NAME}.tar.gz created"

# If a storage account URL was provided, upload the archive to the storage account:
if [ -n "$STORAGE_ACCOUNT_URL" ]; then
    echo "Uploading archive to $STORAGE_ACCOUNT_URL/${ARCHIVE_NAME}.tar.gz"
    azcopy cp "${ARCHIVE_NAME}.tar.gz" "$STORAGE_ACCOUNT_URL/${ARCHIVE_NAME}.tar.gz"
fi

# bzip2 saves ~0.5MB, but takes 4s to decompress
# > hyperfine --prepare 'cp ./telegraf.tar.bz2 test.tar.bz2; rm -f test.tar' 'bzip2 -d test.tar.bz2'
# Benchmark 1: bzip2 -d test.tar.bz2
#   Time (mean ± σ):      4.107 s ±  0.162 s    [User: 1.481 s, System: 0.165 s]
#   Range (min … max):    3.820 s …  4.334 s    10 runs
#
# gzip costs a bit in archive size, but decompresses in 0.6s
# > hyperfine --prepare 'cp ./telegraf.tar.gz test.tar.gz; rm -f test.tar' 'gzip -d test.tar.gz'
# Benchmark 1: gzip -d test.tar.gz
#   Time (mean ± σ):     643.0 ms ±  56.6 ms    [User: 228.1 ms, System: 36.1 ms]
#   Range (min … max):   596.0 ms … 741.5 ms    10 runs
