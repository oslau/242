##################
#USEFUL FUNCTIONS#
##################
#detectCores()
#makeCluster()
#stopCluster()
#clusterSetRNGStream()
#clusterEvalQ()
#clusterApplyLB()
#parLapply()

library(parallel)
setwd("/Users/Olivia/Documents/STA 242/Assignment4/Data")
detectCores()	#4
cl = makeCluster(3, type = "FORK")

#########
#TESTING#
#########
n = 100000
test = read.csv('2000.csv', header = TRUE, nrows = 1000)
test1 = read.csv('2001.csv', header = TRUE, nrows = 1000)
##Do this in parallel for all the years?
clusterApply(cl, list(test, test1), function(x, n){x[sample(nrow(x), n, replace = TRUE),]}, n = 10)







stopCluster(cl)