#!/bin/bash
i=0
for folder in "$@"
do
find "$(realpath "$folder")" -type f |gzip >"filelist$i.txt.gz"
i=$((i+1))

done