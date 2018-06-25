
# source("run_mbm.r")
# ========== generating results =================


# lapply, type
mbm.spark.lapply.type <- run.mbm.spark.lapply.type()
p <- mbm.spark.lapply.type %>% plot.box.mbm
filename <- sprintf("%slapply.type.png", dir_path)
ggsave(filename, width=7, height=4)


# lapply, len
mbm.spark.lapply.len <- run.mbm.spark.lapply.len(FAST)
p <- mbm.spark.lapply.len %>% plot.box.mbm
filename <- sprintf("%slapply.len.%s.png", dir_path, if (FAST) "small" else "full")
ggsave(filename, width=7, height=4)


# dapply, type
mbm.dapply.type <- run.mbm.dapply.type()
p <- mbm.dapply.type %>% plot.box.mbm
filename <- sprintf("%sdapply.type.png", dir_path)
ggsave(filename, width=7, height=4)


# dapply, len
mbm.dapply.len <- run.mbm.dapply.len(FAST)
p <- mbm.dapply.len %>% plot.box.mbm
filename <- sprintf("%sdapply.len.%s.png", dir_path, if (FAST) "small" else "full")
ggsave(filename, width=7, height=4)


# dapply, ncol
mbm.dapply.ncol <- run.mbm.dapply.ncol()
p <- mbm.dapply.ncol %>% plot.box.mbm
filename <- sprintf("%sdapply.ncol.png", dir_path)
ggsave(filename, width=7, height=4)


# dapplyCollect, type
mbm.dapplyCollect.type <- run.mbm.dapplyCollect.type()
p <- mbm.dapplyCollect.type %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.type.png", dir_path)
ggsave(filename, width=7, height=4)


# dapplyCollect, len
mbm.dapplyCollect.len <- run.mbm.dapplyCollect.len(FAST)
p <- mbm.dapplyCollect.len %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.len.%s.png", dir_path, if (FAST) "small" else "full")
ggsave(filename, width=7, height=4)


# dapplyCollect, ncol
mbm.dapplyCollect.ncol <- run.mbm.dapplyCollect.ncol()
p <- mbm.dapplyCollect.ncol %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.ncol.png", dir_path)
ggsave(filename, width=7, height=4)


# gapply, nkey
mbm.gapply.nkey <- run.mbm.gapply.nkey()
p <- mbm.gapply.nkey %>% plot.box.mbm
filename <- sprintf("%sgapply.nkey.png", dir_path)
ggsave(filename, width=7, height=4)


# gapply, nrow
mbm.gapply.nrow <- run.mbm.gapply.nrow(FAST)
p <- mbm.gapply.nrow %>% plot.box.mbm
filename <- sprintf("%sgapply.nrow.%s.png", dir_path, if (FAST) "small" else "full")
ggsave(filename, width=7, height=4)


# gapply, keytype
mbm.gapply.keytype <- run.mbm.gapply.keytype()
p <- mbm.gapply.keytype %>% plot.box.mbm
filename <- sprintf("%sgapply.keytype.png", dir_path)
ggsave(filename, width=7, height=4)


# gapplyCollect, nkey
mbm.gapplyCollect.nkey <- run.mbm.gapplyCollect.nkey()
p <- mbm.gapplyCollect.nkey %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.nkey.png", dir_path)
ggsave(filename, width=7, height=4)


# gapplyCollect, nrow
mbm.gapplyCollect.nrow <- run.mbm.gapplyCollect.nrow(FAST)
p <- mbm.gapplyCollect.nrow %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.nrow.%s.png", dir_path, if (FAST) "small" else "full")
ggsave(filename, width=7, height=4)


# gapplyCollect, keytype
mbm.gapplyCollect.keytype <- run.mbm.gapplyCollect.keytype()
p <- mbm.gapplyCollect.keytype %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.keytype.png", dir_path)
ggsave(filename, width=7, height=4)


tmp <- rbind(
	mbm.spark.lapply.type,
	mbm.spark.lapply.len,
	mbm.dapply.type,
	mbm.dapply.len,
	mbm.dapply.ncol,
	mbm.dapplyCollect.type,
	mbm.dapplyCollect.len,
	mbm.dapplyCollect.ncol,
	mbm.gapply.nkey,
	mbm.gapply.nrow,
	mbm.gapply.keytype,
	mbm.gapplyCollect.nkey, 
	mbm.gapplyCollect.nrow,
	mbm.gapplyCollect.keytype)

towrite <- tmp[order(tmp$expr, tmp$time),]
write.csv(towrite, file="results/results.csv", row.names = F)













