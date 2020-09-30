#!/bin/bash

set -e

cat input_file.txt >> output_file.txt

printf "I ran on this OS:\n" >> output_file.txt
# show OS info
cat /etc/os-release >> output_file.txt

printf "\nAnd here is some GPU info:\n" >> output_file.txt
# list PCI devices; show graphics card model
#lspci | grep -i --color 'vga\|3d\|2d' >> output_file.txt
nvidia-smi >> output_file.txt

printf "Hello OSG!\n"
