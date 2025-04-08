#!/bin/bash

#SBATCH --cpus-per-task=2
#SBATCH -w neptune


###############################
# 1. CPU Model and Clock Frequency
###############################
echo "CPU Information:"
if command -v lscpu &> /dev/null; then
    CPU_MODEL=$(lscpu | grep "Model name:" | sed 's/Model name:[ \t]*//')
    CPU_MHZ=$(lscpu | grep "CPU MHz:" | sed 's/CPU MHz:[ \t]*//')
else
    CPU_MODEL=$(grep -m 1 "model name" /proc/cpuinfo | cut -d ':' -f2 | sed 's/^[ \t]*//')
    CPU_MHZ=$(grep -m 1 "cpu MHz" /proc/cpuinfo | cut -d ':' -f2 | sed 's/^[ \t]*//')
fi
echo "  Model: $CPU_MODEL"
echo "  Clock Frequency: $CPU_MHZ MHz"
echo ""

###############################
# 2. Physical CPUs, Cores, and Threads
###############################
if command -v lscpu &> /dev/null; then
    SOCKETS=$(lscpu | grep "Socket(s):" | awk '{print $2}')
    CORES_PER_SOCKET=$(lscpu | grep "Core(s) per socket:" | awk '{print $4}')
    THREADS_PER_CORE=$(lscpu | grep "Thread(s) per core:" | awk '{print $4}')
    TOTAL_THREADS=$(lscpu | grep "^CPU(s):" | awk '{print $2}')
    echo "  Physical CPUs (Sockets): $SOCKETS"
    echo "  Cores per Socket: $CORES_PER_SOCKET"
    echo "  Threads per Core: $THREADS_PER_CORE"
    echo "  Total Hardware Threads: $TOTAL_THREADS"
    echo ""
fi

###############################
# 3. CPU Instruction Set Architecture
###############################
if command -v lscpu &> /dev/null; then
    ARCH=$(lscpu | grep "Architecture:" | awk '{print $2}')
    echo "  Instruction Set Architecture: $ARCH"
fi
echo ""

###############################
# 4. Cache Line Length
###############################
if command -v lscpu &> /dev/null; then
    CACHE_LINE=$(lscpu | grep "Cache line size:" | awk '{print $4}')
    echo "  Cache Line Length: $CACHE_LINE bytes"
fi
echo ""

###############################
# 5. Cache Sizes (L1, L2, L3)
###############################
if command -v lscpu &> /dev/null; then
    L1D_CACHE=$(lscpu | grep "L1d cache:" | awk '{print $3,$4}')
    L1I_CACHE=$(lscpu | grep "L1i cache:" | awk '{print $3,$4}')
    L2_CACHE=$(lscpu | grep "L2 cache:" | awk '{print $3,$4}')
    L3_CACHE=$(lscpu | grep "L3 cache:" | awk '{print $3,$4}')
    echo "Cache Sizes:"
    echo "  L1d Cache: $L1D_CACHE"
    echo "  L1i Cache: $L1I_CACHE"
    echo "  L2 Cache: $L2_CACHE"
    echo "  L3 Cache: $L3_CACHE"
fi
echo ""

###############################
# 6. System RAM
###############################
echo "Memory Information:"
TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
TOTAL_RAM_MB=$((TOTAL_RAM_KB / 1024))
TOTAL_RAM_GB=$(echo "scale=2; $TOTAL_RAM_MB/1024" | bc)
echo "  Total System RAM: ${TOTAL_RAM_KB} kB (~${TOTAL_RAM_MB} MB or ~${TOTAL_RAM_GB} GB)"
echo ""

###############################
# 7 & 8. GPU Information: Count, Model, and GPU RAM
###############################
echo "GPU Information:"
if command -v nvidia-smi &> /dev/null; then
    GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    echo "  Number of NVIDIA GPUs: $GPU_COUNT"
    nvidia-smi --query-gpu=name,memory.total --format=csv,noheader | while IFS=, read -r name mem; do
        echo "    GPU Model: $(echo $name | xargs)"
        echo "    GPU Memory: $(echo $mem | xargs)"
    done
else
    echo "  nvidia-smi not found. Checking for GPU devices via lspci..."
    GPU_INFO=$(lspci | grep -Ei 'VGA|3D|Display')
    if [ -n "$GPU_INFO" ]; then
        echo "  GPU devices detected:"
        echo "$GPU_INFO" | while read -r line; do
            echo "    $line"
        done
    else
        echo "  No GPU information available."
    fi
fi
echo ""

###############################
# 9 & 10. Filesystem Type and Disk Usage for /data
###############################
echo "Filesystem Information for /data:"
if [ -d /data ]; then
    FS_INFO=$(df -T /data | awk 'NR==2')
    FS_TYPE=$(echo $FS_INFO | awk '{print $2}')
    DISK_TOTAL=$(echo $FS_INFO | awk '{print $3}')
    DISK_FREE=$(echo $FS_INFO | awk '{print $5}')
    echo "  Filesystem Type: $FS_TYPE"
    echo "  Total Disk Space on /data: $DISK_TOTAL"
    echo "  Free Disk Space on /data: $DISK_FREE"
else
    echo "  /data directory not found on this system."
fi
echo ""

###############################
# 11. Linux Kernel and Distribution Information
###############################
echo "OS Information:"
KERNEL_VERSION=$(uname -r)
echo "  Linux Kernel Version: $KERNEL_VERSION"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    echo "  Distribution: $PRETTY_NAME"
else
    echo "  Distribution information not available."
fi
echo ""

###############################
# 12. Default Python 3 Interpreter and Version
###############################
echo "Python 3 Information:"
PYTHON_PATH=$(which python3 2>/dev/null)
if [ -n "$PYTHON_PATH" ]; then
    PYTHON_VERSION=$(python3 --version 2>&1)
    echo "  Python 3 Interpreter: $PYTHON_PATH"
    echo "  Python 3 Version: $PYTHON_VERSION"
else
    echo "  Python 3 is not installed or not in the PATH."
fi
