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
samp = clusterApply(cl, list(test, test1), function(x, n){x[sample(nrow(x), n, replace = TRUE),]}, n = 10)

if(!exists("shell")){
	shell = system
}
files = list.files()
lineCount = as.integer(sapply(strsplit(shell("wc -l [12]*.csv", intern = TRUE), " "), `[`, 2)[-length(lineCount)])
lineCount = lineCount[-length(lineCount)]

##NOT IN PARALLEL!!

##in shell
lapply(1:length(lineCount), function(x, lineCount, files, n){
	samp = sample(2:lineCount[x], n, replace = TRUE)
	lineCmd = paste("sed -n '1p;", paste(samp, "p", sep = "", collapse = ";"), "' ", files[x], "> temp.csv",sep = "")
	invisible(shell(lineCmd, intern = TRUE))
	read.csv("temp.csv")
}, lineCount, files, n)

##in R
lapply(1:length(lineCount), function(x, lineCount, files, n){
	samp = sample(2:lineCount[x], n, replace = TRUE)
	con = file(files[x], "r")
	myLines = readLines(con)[c(1, samp)]
	close(con)
	read.csv(textConnection(myLines))
}, lineCount, files, n)

##IN PARALLEL!!


stopCluster(cl)