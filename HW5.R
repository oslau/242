library(parallel)
setwd("/Users/Olivia/Documents/STA 242/Assignment4/Data")
detectCores()	#4
cl = makeCluster(3, type = "FORK")
