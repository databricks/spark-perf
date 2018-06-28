# usage: source("benchmark.r")

library("SparkR")
library("microbenchmark")
library("magrittr")
library("ggplot2")
library("pryr")

dir_path <- "results/"

# For benchmarking spark.lapply, I generate

#     lists 

# 1. with different types (7):
#     Length = 100,
#     Type = integer, 
#            logical, 
#            double, 
#            character1, 
#            character10, 
#            character100, 
#            character1k. 

data.list.type.double <- runif(100)
data.list.type.int <- as.integer(data.list.type.double * 100)
data.list.type.logical <- data.list.type.double < 0.5
data.list.type.char1 <- sapply(1:100, function(x) { "a" })
data.list.type.char10 <- sapply(1:100, function(x) { "0123456789" })
data.list.type.char100 <- sapply(1:100, function(x) { paste(replicate(20, "hello"), collapse = "") })
data.list.type.char1k <- sapply(1:100, function(x) { paste(replicate(200, "hello"), collapse = "") })

# 2. with different lengths (4):
#     Type = integer,
#     Length = 10,
#              100,
#              1k,
#              10k

data.list.len.10 <- 1:10
data.list.len.100 <- 1:100
data.list.len.1k <- 1:1000
data.list.len.10k <- 1:10000

# For benchmarking dapply+rnow/dapplyCollect, I generate

#     data.frame

# 1. with different types (7):
#     nrow = 100,
#     ncol = 1,
#     Type = integer, 
#            logical, 
#            double, 
#            character1, 
#            character10, 
#            character100, 
#            character1k.

data.df.type.double <- data.frame(data.list.type.double) %>% createDataFrame %>% cache
data.df.type.int <- data.frame(data.list.type.int) %>% createDataFrame %>% cache
data.df.type.logical <- data.frame(data.list.type.logical) %>% createDataFrame %>% cache
data.df.type.char1 <- data.frame(data.list.type.char1) %>% createDataFrame %>% cache
data.df.type.char10 <- data.frame(data.list.type.char10) %>% createDataFrame %>% cache
data.df.type.char100 <- data.frame(data.list.type.char100) %>% createDataFrame %>% cache
data.df.type.char1k <- data.frame(data.list.type.char1k) %>% createDataFrame %>% cache
data.df.type.double %>% nrow
data.df.type.int %>% nrow
data.df.type.logical %>% nrow
data.df.type.char1 %>% nrow
data.df.type.char10 %>% nrow
data.df.type.char100 %>% nrow
data.df.type.char1k %>% nrow

# 2. with different lengths (4):
#     ncol = 1,
#     Type = integer,
#     nrow = 10,
#            100,
#            1k,
#            10k

data.df.len.10 <- data.frame(data.list.len.10) %>% createDataFrame %>% cache
data.rdf.len.100 <- data.frame(data.list.len.100)
data.df.len.100 <- data.rdf.len.100 %>% createDataFrame %>% cache
data.df.len.1k <- data.frame(data.list.len.1k) %>% createDataFrame %>% cache
data.df.len.10k <- data.frame(data.list.len.10k) %>% createDataFrame %>% cache
data.df.len.10 %>% nrow
data.df.len.100 %>% nrow
data.df.len.1k %>% nrow
data.df.len.10k %>% nrow

# 3. with different ncols (3):
#     nrow = 100,
#     Type = integer,
#     ncol = 1,
#            10,
#            100

data.df.ncol.1 <- data.df.len.100
data.df.ncol.10 <- data.frame(rep(data.rdf.len.100, each = 10)) %>% createDataFrame %>% cache
data.df.ncol.100 <- data.frame(rep(data.rdf.len.100, each = 100)) %>% createDataFrame %>% cache
data.df.ncol.1 %>% nrow
data.df.ncol.10 %>% nrow
data.df.ncol.100 %>% nrow

# For benchmarking gapply+rnow/gapplyCollect, I generate

#     data.frame

# 1. with different number of keys (3):
#     ncol = 2,
#     nrow = 1k,
#     Type = <integer, double>,
#     nkeys = 10,
#             100,
#             1000

data.rand.1k <- runif(1000)

data.df.nkey.10 <- data.frame(key = rep(1:10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nkey.100 <- data.frame(key = rep(1:100, 10), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nkey.1k <- data.frame(key = 1:1000, val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nkey.10 %>% cache
data.df.nkey.100 %>% cache
data.df.nkey.1k %>% cache

# data.df.nkey.10 <- repartition(data.df.nkey.10, numPartitions = 10, col = data.df.nkey.10$"key")
# data.df.nkey.100 <- repartition(data.df.nkey.100, numPartitions = 100, col = data.df.nkey.100$"key")
# data.df.nkey.1k <- repartition(data.df.nkey.1k, numPartitions = 100, col = data.df.nkey.1k$"key")


# 2. with different lengths (4):
#     ncol = 2,
#     Type = <integer, double>,
#     nkeys = 10,
#     nrow = 10,
#            100,
#            1k,
#            10k

data.df.nrow.10 <- data.frame(key = 1:10, val = runif(10)) %>% createDataFrame %>% cache
data.df.nrow.100 <- data.frame(key = rep(1:10, 10), val = runif(100)) %>% createDataFrame %>% cache
data.df.nrow.1k <- data.frame(key = rep(1:10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nrow.10k <- data.frame(key = rep(1:10, 1000), val = runif(10000)) %>% createDataFrame %>% cache
data.df.nrow.10 %>% cache
data.df.nrow.100 %>% cache
data.df.nrow.1k %>% cache
data.df.nrow.10k %>% cache

# 3. with different key types (3):
#     ncol = 2,
#     nkeys = 10,
#     nrow = 1k,
#     Type = <integer, double>
#            <character10, double>
#            <character100, double>

key.char10 <- sapply(1:10, function(x) { sprintf("%010d", x) })
key.char100 <- sapply(1:10, function(x) { sprintf("%0100d", x) })

data.df.keytype.int <- data.df.nrow.1k
data.df.keytype.char10 <- data.frame(key = rep(key.char10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.keytype.char100 <- data.frame(key = rep(key.char100, 10), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.keytype.int %>% cache
data.df.keytype.char10 %>% cache
data.df.keytype.char100 %>% cache

# ========== benchmark functions ==============

# plot utils
plot.box.mbm <- function(mbm_result) {
	ggplot(mbm_result, aes(x = expr, y = time/1000000)) + 
	geom_boxplot(outlier.colour = "red", outlier.shape = 1) + labs(x = "", y = "Time (milliseconds)") +
	geom_jitter(position=position_jitter(0.2), alpha = 0.2) + 
	coord_flip()
}

# dummy function
func.dummy <- function(x) { x }

# group function 
gfunc.mean <- function(key, sdf) {
	y <- data.frame(key, mean(sdf$val), stringsAsFactors = F)
	names(y) <- c("key", "val")
	y
}

# lapply, type
run.mbm.spark.lapply.type <- function() {
	microbenchmark(
		"lapply.int.100" = { spark.lapply(data.list.type.int, func.dummy) },
		"lapply.double.100" = { spark.lapply(data.list.type.double, func.dummy) },
		"lapply.logical.100" = { spark.lapply(data.list.type.logical, func.dummy) },
		"lapply.char1.100" = { spark.lapply(data.list.type.char1, func.dummy) },
		"lapply.char10.100" = { spark.lapply(data.list.type.char10, func.dummy) },
		"lapply.char100.100" = { spark.lapply(data.list.type.char100, func.dummy) },
		"lapply.char1k.100" = { spark.lapply(data.list.type.char1k, func.dummy) },
		times = 20
	)
}

# lapply, len
run.mbm.spark.lapply.len <- function(fast) {
	if (fast) {
		microbenchmark(
			"lapply.len.10" = { spark.lapply(data.list.len.10, func.dummy) },
			"lapply.len.100" = { spark.lapply(data.list.len.100, func.dummy) },
			"lapply.len.1k" = { spark.lapply(data.list.len.1k, func.dummy) },
			times = 20
		)
	} else {
		microbenchmark(
			"lapply.len.10" = { spark.lapply(data.list.len.10, func.dummy) },
			"lapply.len.100" = { spark.lapply(data.list.len.100, func.dummy) },
			"lapply.len.1k" = { spark.lapply(data.list.len.1k, func.dummy) },
			"lapply.len.10k" = { spark.lapply(data.list.len.10k, func.dummy) },
			times = 20
		)
	}
	
}

# dapply, type
run.mbm.dapply.type <- function() {
	microbenchmark(
		"dapply.int.100" = { dapply(data.df.type.int, func.dummy, schema(data.df.type.int)) %>% nrow },
		"dapply.double.100" = { dapply(data.df.type.double, func.dummy, schema(data.df.type.double)) %>% nrow },
		"dapply.logical.100" = { dapply(data.df.type.logical, func.dummy, schema(data.df.type.logical)) %>% nrow },
		"dapply.char1.100" = { dapply(data.df.type.char1, func.dummy, schema(data.df.type.char1)) %>% nrow },
		"dapply.char10.100" = { dapply(data.df.type.char10, func.dummy, schema(data.df.type.char10)) %>% nrow },
		"dapply.char100.100" = { dapply(data.df.type.char100, func.dummy, schema(data.df.type.char100)) %>% nrow },
		"dapply.char1k.100" = { dapply(data.df.type.char1k, func.dummy, schema(data.df.type.char1k)) %>% nrow },
		times = 20
	)
}

# dapply, len
run.mbm.dapply.len <- function(fast) {
	if (fast) {
		microbenchmark(
			"dapply.len.10" = { dapply(data.df.len.10, func.dummy, schema(data.df.len.10)) %>% nrow },
			"dapply.len.100" = { dapply(data.df.len.100, func.dummy, schema(data.df.len.100)) %>% nrow },
			"dapply.len.1k" = { dapply(data.df.len.1k, func.dummy, schema(data.df.len.1k)) %>% nrow },
			times = 20
		)
	} else {
		microbenchmark(
			"dapply.len.10" = { dapply(data.df.len.10, func.dummy, schema(data.df.len.10)) %>% nrow },
			"dapply.len.100" = { dapply(data.df.len.100, func.dummy, schema(data.df.len.100)) %>% nrow },
			"dapply.len.1k" = { dapply(data.df.len.1k, func.dummy, schema(data.df.len.1k)) %>% nrow },
			"dapply.len.10k" = { dapply(data.df.len.10k, func.dummy, schema(data.df.len.10k)) %>% nrow },
			times = 20
		)
	}
}

# dapply, ncol
run.mbm.dapply.ncol <- function() {
	microbenchmark(
		"dapply.ncol.1" = { dapply(data.df.ncol.1, func.dummy, schema(data.df.ncol.1)) %>% nrow },
		"dapply.ncol.10" = { dapply(data.df.ncol.10, func.dummy, schema(data.df.ncol.10)) %>% nrow },
		"dapply.ncol.100" = { dapply(data.df.ncol.100, func.dummy, schema(data.df.ncol.100)) %>% nrow },
		times = 20
	)
}

# dapplyCollect, type
run.mbm.dapplyCollect.type <- function() {
	microbenchmark(
		"dapplyCollect.int.100" = { dapplyCollect(data.df.type.int, func.dummy) },
		"dapplyCollect.double.100" = { dapplyCollect(data.df.type.double, func.dummy) },
		"dapplyCollect.logical.100" = { dapplyCollect(data.df.type.logical, func.dummy) },
		"dapplyCollect.char1.100" = { dapplyCollect(data.df.type.char1, func.dummy) },
		"dapplyCollect.char10.100" = { dapplyCollect(data.df.type.char10, func.dummy) },
		"dapplyCollect.char100.100" = { dapplyCollect(data.df.type.char100, func.dummy) },
		"dapplyCollect.char1k.100" = { dapplyCollect(data.df.type.char1k, func.dummy) },
		times = 20
	)
}

# dapplyCollect, len
run.mbm.dapplyCollect.len <- function(fast) {
	if (fast) {
		microbenchmark(
			"dapplyCollect.len.10" = { dapplyCollect(data.df.len.10, func.dummy) },
			"dapplyCollect.len.100" = { dapplyCollect(data.df.len.100, func.dummy) },
			"dapplyCollect.len.1k" = { dapplyCollect(data.df.len.1k, func.dummy) },
			times = 20
		)
	} else {
		microbenchmark(
			"dapplyCollect.len.10" = { dapplyCollect(data.df.len.10, func.dummy) },
			"dapplyCollect.len.100" = { dapplyCollect(data.df.len.100, func.dummy) },
			"dapplyCollect.len.1k" = { dapplyCollect(data.df.len.1k, func.dummy) },
			"dapplyCollect.len.10k" = { dapplyCollect(data.df.len.10k, func.dummy) },
			times = 20
		)
	}
}

# dapplyCollect, ncol
run.mbm.dapplyCollect.ncol <- function() {
	microbenchmark(
		"dapplyCollect.ncol.1" = { dapplyCollect(data.df.ncol.1, func.dummy) },
		"dapplyCollect.ncol.10" = { dapplyCollect(data.df.ncol.10, func.dummy) },
		"dapplyCollect.ncol.100" = { dapplyCollect(data.df.ncol.100, func.dummy) },
		times = 20
	)
}


# gapply, nkey
run.mbm.gapply.nkey <- function() {
	microbenchmark(
		"gapply.nkey.10" = { gapply(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean, schema(data.df.nkey.10)) %>% nrow },
		"gapply.nkey.100" = { gapply(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean, schema(data.df.nkey.100)) %>% nrow },
		"gapply.nkey.1k" = { gapply(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean, schema(data.df.nkey.1k)) %>% nrow },
		times = 20
	)
}


# gapply, nrow
run.mbm.gapply.nrow <- function(fast) {
	if (fast) {
		microbenchmark(
			"gapply.nrow.10" = { gapply(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean, schema(data.df.nrow.10)) %>% nrow },
			"gapply.nrow.100" = { gapply(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean, schema(data.df.nrow.100)) %>% nrow },
			"gapply.nrow.1k" = { gapply(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean, schema(data.df.nrow.1k)) %>% nrow },
			times = 20
		)
	} else {
		microbenchmark(
			"gapply.nrow.10" = { gapply(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean, schema(data.df.nrow.10)) %>% nrow },
			"gapply.nrow.100" = { gapply(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean, schema(data.df.nrow.100)) %>% nrow },
			"gapply.nrow.1k" = { gapply(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean, schema(data.df.nrow.1k)) %>% nrow },
			"gapply.nrow.10k" = { gapply(data.df.nrow.10k, data.df.nrow.10k$key, gfunc.mean, schema(data.df.nrow.10k)) %>% nrow },
			times = 20
		)
	}
	
}

# gapply, keytype
run.mbm.gapply.keytype <- function() {
	microbenchmark(
		"gapply.keytype.int" = { gapply(data.df.keytype.int, data.df.keytype.int$key, gfunc.mean, schema(data.df.keytype.int)) %>% nrow },
		"gapply.keytype.char10" = { gapply(data.df.keytype.char10, data.df.keytype.char10$key, gfunc.mean, schema(data.df.keytype.char10)) %>% nrow },
		"gapply.keytype.char100" = { gapply(data.df.keytype.char100, data.df.keytype.char100$key, gfunc.mean, schema(data.df.keytype.char100)) %>% nrow },
		times = 20
	)
}



# gapplyCollect, nkey
run.mbm.gapplyCollect.nkey <- function() {
	microbenchmark(
		"gapplyCollect.nkey.10" = { gapplyCollect(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean) },
		"gapplyCollect.nkey.100" = { gapplyCollect(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean) },
		"gapplyCollect.nkey.1k" = { gapplyCollect(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean) },
		times = 20
	)
}


# gapplyCollect, nrow
run.mbm.gapplyCollect.nrow <- function(fast) {
	if (fast) {
		microbenchmark(
			"gapplyCollect.nrow.10" = { gapplyCollect(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean) },
			"gapplyCollect.nrow.100" = { gapplyCollect(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean) },
			"gapplyCollect.nrow.1k" = { gapplyCollect(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean) },
			times = 20
		)
	} else {
		microbenchmark(
			"gapplyCollect.nrow.10" = { gapplyCollect(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean) },
			"gapplyCollect.nrow.100" = { gapplyCollect(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean) },
			"gapplyCollect.nrow.1k" = { gapplyCollect(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean) },
			"gapplyCollect.nrow.10k" = { gapplyCollect(data.df.nrow.10k, data.df.nrow.10k$key, gfunc.mean) },
			times = 20
		)
	}
	
}

# gapplyCollect, keytype
run.mbm.gapplyCollect.keytype <- function() {
	microbenchmark(
		"gapplyCollect.keytype.int" = { gapplyCollect(data.df.keytype.int, data.df.keytype.int$key, gfunc.mean) },
		"gapplyCollect.keytype.char10" = { gapplyCollect(data.df.keytype.char10, data.df.keytype.char10$key, gfunc.mean) },
		"gapplyCollect.keytype.char100" = { gapplyCollect(data.df.keytype.char100, data.df.keytype.char100$key, gfunc.mean) },
		times = 20
	)
}






























