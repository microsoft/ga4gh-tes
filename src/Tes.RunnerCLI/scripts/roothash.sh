#!/bin/bash

filename=$1
blocksize=${2:-4194304} # 4 MiB by default

if [ ! -f "$filename" ]; then
    echo "File not found!"
    exit 1
fi

filesize=$(stat -c%s "$filename")
numblocks=$(((filesize + blocksize - 1) / blocksize))

hashList=""

for ((i=0; i<$numblocks; i++)); do
    hashList+=$(dd if="$filename" bs=$blocksize skip=$i count=1 status=none | md5sum | tr -d '\n-' | tr -d ' ')
done

echo -n $hashList | md5sum