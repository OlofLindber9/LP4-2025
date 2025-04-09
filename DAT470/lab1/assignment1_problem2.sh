#!/bin/bash
# collect_system_info.sh
#
# This script collects system information about the following:
#   - The CPU model and clock frequency
#   - The number of physical CPUs (sockets), the number of cores, and hardware threads
#   - The CPU instruction set architecture
#   - The cache line length, and the amount of L1, L2, and L3 cache
#   - The amount of system RAM
#   - The number of GPUs, GPU model(s) and GPU RAM (if any)
#   - The type of filesystem mounted on /data and the total/free disk space there
#   - The running Linux kernel version and the GNU/Linux distribution info
#   - The default Python 3 interpreter location and its version
#
# This script is intended to be executed on minerva (the login node) and via Slurm on the
# remote computers. We choose one from the first group ("callisto") and one from the second ("uranus").
#
# When executed without arguments, it collects data locally on minerva, then uses srun 
# (with the -w option) to run itself in "collect" mode on callisto and uranus.
#
# The output from each system is stored in a temporary directory and then combined into a table.
#
# IMPORTANT: You cannot log in directly to the compute nodes; you must use Slurm (srun in this case)
# to run commands on them.
#
# Usage:
#   ./collect_system_info.sh
#
# (See also the guides and instructions at https://git.chalmers.se/karppa/minerva/)

#-----------------------------
# Function: collect_info
# When invoked with the argument "collect" the script prints out system info.
# This function is used both locally and by the remote Slurm jobs.
collect_info() {
  # CPU model and clock frequency (using lscpu)
  CPU_MODEL=$(lscpu | grep -m1 "Model name:" | cut -d':' -f2 | sed 's/^ //')
  CPU_CLOCK=$(lscpu | grep -m1 "MHz:" | awk '{print $3}')
  
  # Number of physical CPUs (sockets), total cores and threads information.
  SOCKETS=$(lscpu | grep -m1 "Socket(s):" | awk '{print $2}')
  CORES_TOTAL=$(lscpu | grep -m1 "^CPU(s):" | awk '{print $2}')
  THREADS_PER_CORE=$(lscpu | grep -m1 "Thread(s) per core:" | awk '{print $4}')
  
  # For clarity in the report we include a summary value for cores and threads.
  # (One may alternatively list “cores per socket”, etc.)
  CORES_INFO="${CORES_TOTAL} total cores across ${SOCKETS} socket(s)"
  THREADS_INFO="Threads per core: ${THREADS_PER_CORE}"
  
  # Instruction set architecture
  ARCH=$(lscpu | grep -m1 "Architecture:" | awk '{print $2}')
  
  # Cache information: using getconf for the cache line and lscpu for cache sizes.
  CACHE_LINE=$(getconf LEVEL1_DCACHE_LINESIZE 2>/dev/null)
  L1_CACHE=$(lscpu | grep -m1 "L1d:" | awk '{print $3}')
  L2_CACHE=$(lscpu | grep -m1 "L2:" | awk '{print $3}')
  L3_CACHE=$(lscpu | grep -m1 "L3:" | awk '{print $3}')
  
  # Amount of system RAM (from /proc/meminfo, in kB)
  SYS_RAM=$(grep -m1 "MemTotal:" /proc/meminfo | awk '{print $2,$3}')
  
  # GPU information (if nvidia-smi is present)
  if command -v nvidia-smi >/dev/null 2>&1 ; then
    GPU_INFO=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader | sed ':a;N;$!ba;s/\n/; /g')
    GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l | tr -d ' ')
  else
    GPU_INFO="None"
    GPU_COUNT=0
  fi
  
  # Filesystem type on /data and disk usage (total and free space)
  FS_TYPE=$(df -T /data 2>/dev/null | tail -1 | awk '{print $2}')
  DISK_TOTAL=$(df -h /data 2>/dev/null | tail -1 | awk '{print $2}')
  DISK_FREE=$(df -h /data 2>/dev/null | tail -1 | awk '{print $4}')
  
  # Linux kernel version and OS distribution info
  KERNEL=$(uname -r)
  if command -v lsb_release >/dev/null 2>&1 ; then
    OS_DIST=$(lsb_release -d | cut -d':' -f2 | sed 's/^ //')
  else
    # Fall back on /etc/os-release if lsb_release is not available
    OS_DIST=$(grep PRETTY_NAME /etc/os-release | cut -d'=' -f2 | tr -d '"')
  fi
  
  # Python default interpreter: its location and version info.
  PYTHON_PATH=$(which python3 2>/dev/null)
  if [ -n "$PYTHON_PATH" ]; then
    PYTHON_VER=$("$PYTHON_PATH" --version 2>&1)
  else
    PYTHON_VER="Not found"
  fi
  
  # Print the gathered information in KEY: VALUE format.
  echo "CPU_Model: ${CPU_MODEL}"
  echo "CPU_Clock: ${CPU_CLOCK} MHz"
  echo "Physical_CPUs: ${SOCKETS}"
  echo "Cores: ${CORES_INFO}"
  echo "Threads: ${THREADS_INFO}"
  echo "Architecture: ${ARCH}"
  echo "Cache_Line: ${CACHE_LINE} bytes"
  echo "L1_Cache: ${L1_CACHE}"
  echo "L2_Cache: ${L2_CACHE}"
  echo "L3_Cache: ${L3_CACHE}"
  echo "System_RAM: ${SYS_RAM}"
  echo "GPUs: ${GPU_COUNT} - ${GPU_INFO}"
  echo "GPU_RAM: ${GPU_INFO}"  # (GPU memory is already included in GPU_INFO output)
  echo "Filesystem: ${FS_TYPE}"
  echo "Disk_Total: ${DISK_TOTAL}"
  echo "Disk_Free: ${DISK_FREE}"
  echo "Kernel: ${KERNEL}"
  echo "OS: ${OS_DIST}"
  echo "Python: ${PYTHON_PATH} (${PYTHON_VER})"
}

# If the script is called with "collect" as first argument, run the collection function.
if [ "$1" == "collect" ]; then
  collect_info
  exit 0
fi

#---------------------------------------------------------------------------------
# MAIN SCRIPT: Run on minerva to collect local info and use Slurm (srun) to run
# the same info collection on the selected remote nodes.
# We chose "callisto" (from the io/europa/ganymede/callisto group) and "uranus" (from the uranus/neptune group).
#---------------------------------------------------------------------------------

# Create a temporary directory to hold output files.
OUTPUT_DIR="system_info_output"
mkdir -p "${OUTPUT_DIR}"

echo "Collecting information on minerva..."
bash "$0" collect > "${OUTPUT_DIR}/minerva.txt"

echo "Collecting information on callisto via Slurm..."
srun -w callisto bash "$0" collect > "${OUTPUT_DIR}/callisto.txt"

echo "Collecting information on uranus via Slurm..."
srun -w uranus bash "$0" collect > "${OUTPUT_DIR}/uranus.txt"

# Define the list of fields (keys) in the order they were printed.
FIELDS=( "CPU_Model" "CPU_Clock" "Physical_CPUs" "Cores" "Threads" \
         "Architecture" "Cache_Line" "L1_Cache" "L2_Cache" "L3_Cache" \
         "System_RAM" "GPUs" "GPU_RAM" "Filesystem" "Disk_Total" "Disk_Free" \
         "Kernel" "OS" "Python" )

# Output header.
printf "\n%-20s %-40s %-40s %-40s\n" "Field" "minerva" "callisto" "uranus"
printf "%0.s-" {1..140}
echo

# For each field, extract the value from the respective output files and print a formatted row.
for field in "${FIELDS[@]}"; do
  minerva_val=$(grep "^${field}:" "${OUTPUT_DIR}/minerva.txt" | cut -d':' -f2- | sed 's/^ //')
  callisto_val=$(grep "^${field}:" "${OUTPUT_DIR}/callisto.txt" | cut -d':' -f2- | sed 's/^ //')
  uranus_val=$(grep "^${field}:" "${OUTPUT_DIR}/uranus.txt" | cut -d':' -f2- | sed 's/^ //')
  printf "%-20s %-40s %-40s %-40s\n" "$field" "$minerva_val" "$callisto_val" "$uranus_val"
done