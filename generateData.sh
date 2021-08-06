#!/bin/sh

filename="/tmp/big.csv"
>${filename}
for count in $(seq 10000)
do
  # shellcheck disable=SC2004
  echo $(( $RANDOM % 100000 + 1628100000 )),"${count}",$(echo "${count} + 1000000" | bc),$(echo "scale=8; $RANDOM/100" | bc ),$(echo "scale=8; $RANDOM/100" | bc ) >> $filename
done