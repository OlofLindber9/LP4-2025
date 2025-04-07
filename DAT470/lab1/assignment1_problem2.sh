#! /bin/bash
#SBATCH -w neptune

cpu_model = $(lscpu | grep "Model name:")
cpu_mhz = $(lscpu | grep "CPU MHz:")

echo "Model: $cpu_model"
echo "Clock Frequency: $cpu_mhz"