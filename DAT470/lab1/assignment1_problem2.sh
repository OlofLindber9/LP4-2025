#!/bin/bash
#SBATCH -w callisto,neptune

collect_info() {
  CPU_MODEL=$(lscpu | grep -m1 "Model name:" | cut -d':' -f2 | sed 's/^ //')
  CPU_CLOCK=$(lscpu | grep "max MHz" | cut -d ":" -f2 | sed 's/^ //')
  SOCKETS=$(lscpu | grep -m1 "Socket(s):" | awk '{print $2}')
  CORES_TOTAL=$(lscpu | grep -m1 "^CPU(s):" | awk '{print $2}')
  THREADS_PER_CORE=$(lscpu | grep -m1 "Thread(s) per core:" | awk '{print $4}')
  ARCH=$(lscpu | grep -m1 "Architecture:" | awk '{print $2}')
  CACHE_LINE=$(getconf LEVEL1_DCACHE_LINESIZE 2>/dev/null)
  L1_CACHE=$(lscpu | grep -m1 "L1d cache:" | awk '{print $3}')
  L2_CACHE=$(lscpu | grep -m1 "L2 cache:" | awk '{print $3}')
  L3_CACHE=$(lscpu | grep -m1 "L3 cache:" | awk '{print $3}')
  SYS_RAM=$(grep -m1 "MemTotal:" /proc/meminfo | awk '{print $2,$3}')
  GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -n 1 2>/dev/null || echo "N/A")
  GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | wc -l || echo "N/A")
  GPU_RAM=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader | head -n 1 2>/dev/null || echo "N/A")
  FS_TYPE=$(df -T /data 2>/dev/null | awk 'NR==2 {print $2}')
  DISK_TOTAL=$(df -h /data 2>/dev/null | awk 'NR==2 {print $2}')
  DISK_FREE=$(df -h /data 2>/dev/null | awk 'NR==2 {print $4}')
  KERNEL=$(uname -r)
  OS_DIST=$(lsb_release -d 2>/dev/null | cut -d':' -f2 | sed 's/^ //' || echo "N/A")
  PYTHON_PATH=$(which python3)
  PYTHON_VERSION=$("$PYTHON_PATH" --version 2>/dev/null | awk '{print $2}' )

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
  echo "# GPUs: ${GPU_COUNT}"
  echo "GPU RAM: ${GPU_RAM}"
  echo "Filesystem Type: ${FS_TYPE}"
  echo "Disk Total: ${DISK_TOTAL}"
  echo "Disk Free: ${DISK_FREE}"
  echo "Kernel: ${KERNEL}"
  echo "OS: ${OS_DIST}"
  echo "Python: ${PYTHON_PATH} (${PYTHON_VERSION})"
}

parse_info() {
  local label=$1
  local info_str="$2"
  declare -A parsed

  while IFS=":" read -r key value; do
    parsed["$key"]=$(echo "$value" | sed 's/^ //')
  done <<< "$info_str"

  echo "${parsed["CPU Model"]:-N/A}"
  echo "${parsed["CPU Clock Frequency"]:-N/A}"
  echo "${parsed["# Physical CPUs"]:-N/A}"
  echo "${parsed["# CPU Cores"]:-N/A}"
  echo "${parsed["# Threads / Core"]:-N/A}"
  echo "${parsed["Architecture"]:-N/A}"
  echo "${parsed["Cache Line Length"]:-N/A}"
  echo "${parsed["L1 Cache"]:-N/A}"
  echo "${parsed["L2 Cache"]:-N/A}"
  echo "${parsed["L3 Cache"]:-N/A}"
  echo "${parsed["System RAM"]:-N/A}"
  echo "${parsed["GPU Model"]:-N/A}"
  echo "${parsed["# GPUs"]:-N/A}"
  echo "${parsed["GPU RAM"]:-N/A}"
  echo "${parsed["Filesystem Type"]:-N/A}"
  echo "${parsed["Disk Total"]:-N/A}"
  echo "${parsed["Disk Free"]:-N/A}"
  echo "${parsed["Kernel"]:-N/A}"
  echo "${parsed["OS"]:-N/A}"
  echo "${parsed["Python"]:-N/A}"
}

# Run once per task per node
info=$(collect_info)

# Store output in temp file (shared dir or $SLURM_TMPDIR)
OUTDIR="${SLURM_SUBMIT_DIR:-.}/sysinfo_tmp"
mkdir -p "$OUTDIR"
hostname_short=$(hostname -s)
echo "$info" > "$OUTDIR/info_$hostname_short.txt"

# Barrier to wait for all tasks
scontrol show hostname | grep -q "$(hostname)" && sleep 5

# Only one task (first) assembles the final table
if [[ "$(hostname -s)" == "login" ]]; then
  sleep 10  # Give others time to write output
  declare -A info_map
  declare -a nodes=("login" "callisto" "neptune")
  declare -a keys=(
    "CPU Model"
    "CPU Clock Frequency"
    "# Physical CPUs"
    "# CPU Cores"
    "# Threads / Core"
    "Architecture"
    "Cache Line Length"
    "L1 Cache"
    "L2 Cache"
    "L3 Cache"
    "System RAM"
    "GPU Model"
    "# GPUs"
    "GPU RAM"
    "Filesystem Type"
    "Disk Total"
    "Disk Free"
    "Kernel"
    "OS"
    "Python"
  )

  for node in "${nodes[@]}"; do
    declare -A node_info
    while IFS=":" read -r key value; do
      key=$(echo "$key" | sed 's/^ *//;s/ *$//')
      value=$(echo "$value" | sed 's/^ *//;s/ *$//')
      [[ "$key" == "Node" ]] && continue
      node_info["$key"]="$value"
    done < "$OUTDIR/info_$node.txt"
    for k in "${keys[@]}"; do
      info_map["$k,$node"]="${node_info[$k]:-N/A}"
    done
  done

  # Print table
  printf "%-25s %-25s %-25s %-25s\n" "Property" "login" "callisto" "neptune"
  printf "%s\n" "-------------------------------------------------------------------------------------------------------------"
  for k in "${keys[@]}"; do
    printf "%-25s %-25s %-25s %-25s\n" "$k" \
      "${info_map[$k,login]}" \
      "${info_map[$k,callisto]}" \
      "${info_map[$k,neptune]}"
  done
fi