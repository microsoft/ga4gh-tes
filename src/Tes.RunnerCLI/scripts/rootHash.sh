#!/bin/bash

filename=$1
blocksize=4194304 # 4 MiB

if [ ! -f "$filename" ]; then
    echo "File not found!"
    exit 1
fi

filesize=$(stat -c%s "$filename")
numblocks=$(((filesize + blocksize - 1) / blocksize))

rm .hashList

for ((i=0; i<$numblocks; i++)); do
    (dd if="$filename" bs=$blocksize skip=$i count=1 status=none | md5sum | tr -d '\n-' | tr -d ' ') >> .hashList

done

md5sum .hashList
