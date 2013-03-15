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

##############
#WORKING CODE#
##############

library(parallel)
library(randomForest)
library(snow)
library(RSQLite)
Sys.setlocale(locale = "C")
setwd("/Users/Olivia/Documents/STA 242/Assignment4/Data")
detectCores()	#4
cl = makeCluster(3, type = "FORK")
clusterSetRNGStream(cl)

n = 100000
if(!exists("shell")){
	shell = system
}
files = list.files()[1:22]
mydat = clusterApplyLB(cl, files, function(x, n){
	lineCount = as.integer(strsplit(shell(paste("wc -l ", x, sep = ""), intern = TRUE), " ")[[1]][2])
	samp = sample(2:lineCount, n, replace = TRUE)
	con = file(x, "r")
	myLines = readLines(con)[c(1, samp)]
	close(con)
	read.csv(textConnection(myLines))
}, n = 2)

mydat = clusterApplyLB(cl, files, function(x, n){
	lineCount = as.integer(strsplit(shell(paste("wc -l ", x, sep = ""), intern = TRUE), " ")[[1]][2])
	samp = sample(2:lineCount, n, replace = TRUE)
	lineCmd = paste("echo \"",paste(samp, "p", sep = "", collapse = ";"), "\"|xargs -0 -I {} sed -n {} ", x, sep = "")
	read.csv(textConnection(shell(lineCmd, intern = TRUE)), header=FALSE)
}, n)
stopCluster(cl)
mydat = do.call("rbind", mydat)
colnames(mydat)<- strsplit(readLines("2001.csv", 1), ",")[[1]]

#########
#TESTING#
#########

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

#To Do: 
#look at power to compare sample size?
#randomize after sampling - might be sorted
#implement in sqlite3
dr = dbDriver("SQLite")
con = dbConnect(dr, dbname = "airline")
datSQL = dbSendQuery(con, "SELECT * FROM delays")
fetch(datSQL) ##Now...somehow grab certain lines...hm...
#randomforest!
