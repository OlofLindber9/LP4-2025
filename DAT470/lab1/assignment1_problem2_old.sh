#!/bin/bash
#SBATCH --nodelist=europa,neptune
#SBATCH --ntasks=2

srun bash -c "hostname && lscpu | grep 'Model name' && lscpu | grep 'MHz'"