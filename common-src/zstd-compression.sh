#!/bin/sh
#
# script "zstd-compression3", use with server_custom_compress "/whatever/zstd-compression3 :

if [[ "$1" == "-d" ]]; then
    zstd -dqcf
else
    zstd -qc -3 -T0
fi
