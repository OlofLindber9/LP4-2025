#!/bin/bash

collect_info() {
  # The model of and the clock frequency of the CPU
  CPU_MODEL=$(lscpu | grep -m1 "Model name:" | cut -d':' -f2 | sed 's/^ //')
  CPU_CLOCK=$(lscpu | lscpu | grep "max MHz" | cut -d ":" -f2 | sed 's/^ //') # Does not work for remote nodes

  # The number of physical CPUs (sockets in use)
  SOCKETS=$(lscpu | grep -m1 "Socket(s):" | awk '{print $2}')
  # the number of cores
  CORES_TOTAL=$(lscpu | grep -m1 "^CPU(s):" | awk '{print $2}')
  # the number of hardware threads
  THREADS_PER_CORE=$(lscpu | grep -m1 "Thread(s) per core:" | awk '{print $4}')
  
  # Instruction set architecture
  ARCH=$(lscpu | grep -m1 "Architecture:" | awk '{print $2}')
  
  # The cache line length
  CACHE_LINE=$(getconf LEVEL1_DCACHE_LINESIZE 2>/dev/null)
  # The amount of L1, L2, and L3 cache
  L1_CACHE=$(lscpu | grep -m1 "L1d cache:" | awk '{print $3}')
  L2_CACHE=$(lscpu | grep -m1 "L2 cache:" | awk '{print $3}')
  L3_CACHE=$(lscpu | grep -m1 "L3 cache:" | awk '{print $3}')
  
  # The amount of system RAM
  SYS_RAM=$(grep -m1 "MemTotal:" /proc/meminfo | awk '{print $2,$3}')

  # The number of GPUs and model of the GPU(s)
  GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -n 1)
  GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)

  # The amount of RAM on the GPU(s)
  GPU_RAM=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader | head -n 1)
  
  # The type of filesystem of /data
  FS_TYPE=$(df -T /data | awk 'NR==2 {print $2}')

  # The total amount of disk space and the amount of free space on /data
  DISK_TOTAL=$(df -h /data | awk 'NR==2 {print $2}')
  DISK_FREE=$(df -h /data | awk 'NR==2 {print $4}')

  # The version of the Linux kernel running on the system 
  KERNEL=$(uname -r)

  # the GNU/Linux distribution and its version running on the system
  OS_DIST=$(lsb_release -d | cut -d':' -f2 | sed 's/^ //')

  # The filename and the version of the default Python 3 interpreter available on the system (globally installed)
  PYTHON_PATH=$(which python3)
  PYTHON_VERSION=$("$PYTHON_PATH" --version | awk '{print $2}' )
  
  # Print the gathered information in KEY: VALUE format.
  echo "CPU Model: ${CPU_MODEL}"
  echo "CPU Clock Frequency: ${CPU_CLOCK} MHz"
  echo "# Physical CPUs: ${SOCKETS}"
  echo "# CPU Cores: ${CORES_TOTAL}"
  echo "# Threads / Core: ${THREADS_PER_CORE}"
  echo "Architecture: ${ARCH}"
  echo "Cache Line Length: ${CACHE_LINE} bytes"
  echo "L1 Cache: ${L1_CACHE}"
  echo "L2 Cache: ${L2_CACHE}"
  echo "L3 Cache: ${L3_CACHE}"
  echo "System RAM: ${SYS_RAM}"
  echo "GPU Model: ${GPU_NAME}"
  echo "# GPUs :${GPU_COUNT}"
  echo "GPU RAM: ${GPU_RAM}"
  echo "Filesystem Type: ${FS_TYPE}"
  echo "Disk Total: ${DISK_TOTAL}"
  echo "Disk Free: ${DISK_FREE}"
  echo "Kernel: ${KERNEL}"
  echo "OS: ${OS_DIST}"
  echo "Python: ${PYTHON_PATH} (${PYTHON_VERSION})"
}

# If called with "collect", just output info and exit
if [[ "$1" == "collect" ]]; then
  collect_info
  exit 0
fi

# Get local info
MINERVA_INFO=$(collect_info)

# Helper: run remotely via sbatch and capture stdout
remote_info() {
  local NODE=$1
  srun -w "$NODE" --ntasks=1 --output=/dev/stdout bash "$0" collect
}

# Capture remote outputs
CALLISTO_INFO=$(remote_info callisto)
URANUS_INFO=$(remote_info uranus)

# Define expected keys in correct order
FIELDS=(
  "CPU_Model" "CPU_Clock" "Physical_CPUs" "Cores" "Threads"
  "Architecture" "Cache_Line" "L1_Cache" "L2_Cache" "L3_Cache"
  "System_RAM" "GPUs" "GPU_RAM" "Filesystem" "Disk_Total" "Disk_Free"
  "Kernel" "OS" "Python"
)

# Print table header
printf "\n%-20s %-40s %-40s %-40s\n" "Field" "minerva" "callisto" "uranus"
printf "%0.s-" {1..140}
echo

# Helper: extract value from raw info based on key
extract() {
  echo "$1" | grep "^$2:" | cut -d':' -f2- | sed 's/^ //'
}

# Print each field
for field in "${FIELDS[@]}"; do
  val1=$(extract "$MINERVA_INFO" "$field")
  val2=$(extract "$CALLISTO_INFO" "$field")
  val3=$(extract "$URANUS_INFO" "$field")
  printf "%-20s %-40s %-40s %-40s\n" "$field" "$val1" "$val2" "$val3"
done