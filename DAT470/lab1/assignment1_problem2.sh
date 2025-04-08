#!/bin/bash
#SBATCH --nodelist=europa,neptune
#SBATCH --ntasks=2

srun lscpu | grep "Model name:"