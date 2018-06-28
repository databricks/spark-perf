#!/bin/bash
# Usage: ./run_benchmark.sh /Users/liang/spark fast

export SPARK_HOME=$1
export R_LIBS_USER=$SPARK_HOME/R/lib

Rscript --vanilla run_benchmark.r $2