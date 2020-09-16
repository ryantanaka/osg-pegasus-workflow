#!/bin/bash

set -e

cat input_file.txt >> output_file.txt
printf "I ran on this OS:\n" >> output_file.txt
cat /etc/os-release >> output_file.txt

printf "Hello OSG!"