# SparkR \*apply() benchmark

This is a performance task for SparkR \*apply API, including spark.lapply, dapply/dapplyCollect, gapply/gapplyCollect.

`benchmark.r` generates data in different types and sizes.

`run_mbm.r` runs tests and save results in results/results.csv and plots in .png files.

## How to run

Open SparkR shell, use

```
source("benchmark.r")
FAST = T   # if you want to run small test
# FAST = F  # if you want to run full test
source("run_mbm.r")
```