#!/bin/bash

# Function to collect system information
collect_info() {
  # CPU Information
  CPU_MODEL=$(lscpu | grep -m1 "Model name:" | cut -d':' -f2 | sed 's/^ //')
  CPU_CLOCK=$(lscpu | grep "max MHz" | cut -d':' -f2 | sed 's/^ //' | cut -d'.' -f1)
  [ -z "$CPU_CLOCK" ] && CPU_CLOCK=$(lscpu | grep "CPU MHz" | cut -d':' -f2 | sed 's/^ //' | cut -d'.' -f1)
  
  SOCKETS=$(lscpu | grep -m1 "Socket(s):" | awk '{print $2}')
  CORES_TOTAL=$(lscpu | grep -m1 "^CPU(s):" | awk '{print $2}')
  THREADS_PER_CORE=$(lscpu | grep -m1 "Thread(s) per core:" | awk '{print $4}')
  ARCH=$(lscpu | grep -m1 "Architecture:" | awk '{print $2}')
  
  CACHE_LINE=$(getconf LEVEL1_DCACHE_LINESIZE 2>/dev/null)
  L1_CACHE=$(lscpu | grep -m1 "L1d cache:" | awk '{print $3}')
  L2_CACHE=$(lscpu | grep -m1 "L2 cache:" | awk '{print $3}')
  L3_CACHE=$(lscpu | grep -m1 "L3 cache:" | awk '{print $3}')
  
  # Memory Information
  SYS_RAM=$(grep -m1 "MemTotal:" /proc/meminfo | awk '{print $2}')
  SYS_RAM_GB=$(echo "scale=1; $SYS_RAM/1024/1024" | bc)
  
  # GPU Information
  if command -v nvidia-smi &>/dev/null; then
    GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -n1)
    GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    GPU_RAM=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader | head -n1)
  else
    GPU_NAME="None"
    GPU_COUNT=0
    GPU_RAM="0 MiB"
  fi
  
  # Filesystem Information
  if df /data &>/dev/null; then
    FS_TYPE=$(df -T /data | awk 'NR==2 {print $2}')
    DISK_TOTAL=$(df -h /data | awk 'NR==2 {print $2}')
    DISK_FREE=$(df -h /data | awk 'NR==2 {print $4}')
  else
    FS_TYPE="N/A"
    DISK_TOTAL="N/A"
    DISK_FREE="N/A"
  fi
  
  # OS Information
  KERNEL=$(uname -r)
  if [ -f /etc/os-release ]; then
    OS_DIST=$(grep PRETTY_NAME /etc/os-release | cut -d'"' -f2)
  elif [ -f /etc/redhat-release ]; then
    OS_DIST=$(cat /etc/redhat-release)
  else
    OS_DIST=$(lsb_release -d 2>/dev/null | cut -d':' -f2 | sed 's/^ //')
  fi
  
  # Python Information
  PYTHON_PATH=$(which python3 2>/dev/null)
  if [ -n "$PYTHON_PATH" ]; then
    PYTHON_VERSION=$("$PYTHON_PATH" --version 2>&1 | awk '{print $2}')
  else
    PYTHON_PATH="Not found"
    PYTHON_VERSION="N/A"
  fi
  
  # Create JSON output
  cat <<EOF
  {
    "CPU Model": "$CPU_MODEL",
    "CPU Clock (MHz)": "$CPU_CLOCK",
    "Sockets": "$SOCKETS",
    "Total Cores": "$CORES_TOTAL",
    "Threads per Core": "$THREADS_PER_CORE",
    "Architecture": "$ARCH",
    "Cache Line (bytes)": "$CACHE_LINE",
    "L1 Cache": "$L1_CACHE",
    "L2 Cache": "$L2_CACHE",
    "L3 Cache": "$L3_CACHE",
    "System RAM (GB)": "$SYS_RAM_GB",
    "GPU Model": "$GPU_NAME",
    "GPU Count": "$GPU_COUNT",
    "GPU RAM": "$GPU_RAM",
    "Filesystem Type": "$FS_TYPE",
    "Disk Total": "$DISK_TOTAL",
    "Disk Free": "$DISK_FREE",
    "Kernel": "$KERNEL",
    "OS": "$OS_DIST",
    "Python Path": "$PYTHON_PATH",
    "Python Version": "$PYTHON_VERSION"
  }
EOF
}

generate_table() {
  local minerva_data="$1"
  local io_data="$2"
  local uranus_data="$3"

  echo ""
  echo "System Information Comparison"
  echo "================================================================================================================"
  printf "%-25s | %-30s | %-30s | %-30s\n" "Property" "Minerva" "io" "uranus"
  echo "----------------------------------------------------------------------------------------------------------------"

  PROPERTIES=(
    "CPU Model" "CPU Clock (MHz)" "Sockets" "Total Cores" "Threads per Core"
    "Architecture" "Cache Line (bytes)" "L1 Cache" "L2 Cache" "L3 Cache"
    "System RAM (GB)" "GPU Model" "GPU Count" "GPU RAM" "Filesystem Type"
    "Disk Total" "Disk Free" "Kernel" "OS" "Python Path" "Python Version"
  )

  for prop in "${PROPERTIES[@]}"; do
    minerva_val=$(echo "$minerva_data" | jq -r ".\"$prop\"" | sed 's/"/\\"/g')
    io_val=$(echo "$io_data" | jq -r ".\"$prop\"" | sed 's/"/\\"/g')
    uranus_val=$(echo "$uranus_data" | jq -r ".\"$prop\"" | sed 's/"/\\"/g')
    
    printf "%-25s | %-30s | %-30s | %-30s\n" "$prop" "$minerva_val" "$io_val" "$uranus_val"
  done

  echo "================================================================================================================"
}

main() {
  HOSTNAME=$(hostname)
  INFO=$(collect_info)

  if [ "$HOSTNAME" = "minerva" ]; then
    # Submit jobs and wait for completion
    echo "Submitting jobs to io and uranus..."
    JOBID_IO=$(sbatch --parsable -N1 -w io --wrap="$(realpath $0)" -o io_output.json -e io_error.log)
    JOBID_URANUS=$(sbatch --parsable -N1 -w uranus --wrap="$(realpath $0)" -o uranus_output.json -e uranus_error.log)

    echo "Waiting for jobs to complete (this may take a few minutes)..."
    while squeue -j $JOBID_IO,$JOBID_URANUS | grep -q -E "$JOBID_IO|$JOBID_URANUS"; do
      sleep 10
    done

    # Give Slurm time to write files
    sleep 5

    # Load data (with fallback to empty JSON if files missing)
    IO_DATA=$(cat io_output.json 2>/dev/null || echo '{}')
    URANUS_DATA=$(cat uranus_output.json 2>/dev/null || echo '{}')

    # Generate and display table
    generate_table "$INFO" "$IO_DATA" "$URANUS_DATA"

    # Clean up
    rm -f io_output.json uranus_output.json io_error.log uranus_error.log
  else
    # Just output JSON when running on worker nodes
    echo "$INFO"
  fi
}

# Check dependencies
if ! command -v jq &>/dev/null; then
  echo "Error: jq is required. Please install with 'sudo apt-get install jq' or similar."
  exit 1
fi

main