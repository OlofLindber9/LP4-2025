#!/bin/bash

# The model of and the clock frequency of the CPU
CPU_MODEL=$(lscpu | grep "Model name" | cut -d ":" -f2 | xargs)
CPU_CLOCK=$(lscpu | grep "max MHz" | cut -d ":" -f2 | xargs)

# The number of physical CPUs (sockets in use), 
SOCKETS=$(lscpu | grep -m1 "Socket(s):" | awk '{print $2}')
# the number of cores, and
CORES_TOTAL=$(lscpu | grep -m1 "^CPU(s):" | awk '{print $2}')
# the number of hardware threads
THREADS_PER_CORE=$(lscpu | grep -m1 "Thread(s) per core:" | awk '{print $4}')

# The instruction set architecture of the CPU
ARCH=$(lscpu | grep -m1 "Architecture:" | awk '{print $2}')

# The cache line length
CACHE_LINE=$(getconf LEVEL1_DCACHE_LINESIZE 2>/dev/null)

# The amount of L1, L2, and L3 cache
L1_CACHE=$(lscpu | grep -m1 "L1d cache:" | awk '{print $3}')
L2_CACHE=$(lscpu | grep -m1 "L2 cache:" | awk '{print $3}')
L3_CACHE=$(lscpu | grep -m1 "L3 cache:" | awk '{print $3}')

# The amount of system RAM
SYS_RAM=$(grep -m1 "MemTotal:" /proc/meminfo | awk '{print $2,$3}')

if command -v nvidia-smi &>/dev/null; then
# The number of GPUs and 
  GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
# model of the GPU(s)
  GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -n1)
# The amount of RAM on the GPU(s)
  GPU_RAM=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader | head -n1)
else
  GPU_NAME="N/A"
  GPU_COUNT=0
  GPU_RAM="N/A"
fi

# The type of filesystem of /data
FS_TYPE=$(df -T /data 2>/dev/null | awk 'NR==2 {print $2}')

# The total amount of disk space and the amount of free space on /data
DISK_TOTAL=$(df -h /data 2>/dev/null | awk 'NR==2 {print $2}')
DISK_FREE=$(df -h /data 2>/dev/null | awk 'NR==2 {print $4}')

# The version of the Linux kernel running on the system and 
KERNEL=$(uname -r)

# the GNU/Linux distribution and its version running on the system
OS_DIST=$(lsb_release -d 2>/dev/null | cut -d':' -f2 | xargs)

# The filename and the version of the default Python 3
PYTHON_PATH=$(which python3)
PYTHON_VERSION=$("$PYTHON_PATH" --version 2>/dev/null | awk '{print $2}' )

# Print all information
echo "CPU Model: ${CPU_MODEL}"
if [ -z "$CPU_CLOCK" ]; then
    echo "CPU Clock Frequency: Missing data"
else
    echo "CPU Clock Frequency: ${CPU_CLOCK} MHz"
fi
echo "Physical CPUs (Sockets): ${SOCKETS}"
echo "CPU Cores: ${CORES_TOTAL}"
echo "Threads / Core: ${THREADS_PER_CORE}"
echo "Architecture: ${ARCH}"
echo "Cache Line Length: ${CACHE_LINE} bytes"
echo "L1 Cache: ${L1_CACHE}"
echo "L2 Cache: ${L2_CACHE}"
echo "L3 Cache: ${L3_CACHE}"
echo "System RAM: ${SYS_RAM}"
echo "GPU Model: ${GPU_NAME}"
echo "GPUs: ${GPU_COUNT}"
echo "GPU RAM: ${GPU_RAM}"
echo "Filesystem Type: ${FS_TYPE}"
echo "Disk Total: ${DISK_TOTAL}"
echo "Disk Free: ${DISK_FREE}"
echo "Kernel: ${KERNEL}"
echo "OS: ${OS_DIST}"
echo "Python: ${PYTHON_PATH} (${PYTHON_VERSION})"