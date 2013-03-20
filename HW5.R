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
Sys.setlocale(locale = "C")
setwd("/Users/Olivia/Documents/STA 242/Assignment4/Data")
#detectCores()	#4
if(!exists("shell")){
	shell = system
}
cl = makeCluster(3, type = "FORK")
clusterSetRNGStream(cl)
files = list.files()[1:22]

##############
#####IN R#####
##############

lineCount = read.table(textConnection(shell("wc -l [12]*.csv", intern = TRUE)), header = FALSE)
mySamples = lapply(lineCount[[1]], function(x, n){
	sample(2:x[1], n, replace = TRUE)
	}, 100000)
samp = lapply(mySamples, sort)

cl = makeCluster(4, type = "FORK")
clusterApply(cl, 1:22, function(x){
	cat(readLines(files[x])[samp[[x]]], file = "samples.txt", sep = "\n", append = TRUE)
})
stopCluster(cl)
mydat = read.csv("samples.txt", header = FALSE)
colnames(mydat)<- strsplit(readLines("2001.csv", 1), ",")[[1]]
#save(mydat, "samples.Rda")
#load("samples.Rda")

#taking a smaller sample to make it no as tedious
mydat = mydat[sample(1:nrow(mydat), 200000), ]	

###CLEANING THE DATA###
mydat = mydat[!is.na(mydat$ArrTime), ]	##Removing Cancelled flights
mydat = mydat[!is.na(mydat$ArrDelay), ]	##Removing Diverted flights
##Removing columns with mostly NA's
nullCols = c("TailNum", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "Cancelled", "Diverted")
mydat = mydat[, -(which(colnames(mydat) %in% nullCols))]

#Group Airports
mainAirports = c("LAX", "OAK", "SFO", "SMF")
mydat[, "Origin"] = as.character(mydat[, "Origin"])
mydat[, "Dest"] = as.character(mydat[, "Dest"])
mydat[-which(mydat$Origin %in% mainAirports), "Origin"] = "Other"
mydat[-which(mydat$Dest %in% mainAirports), "Dest"] = "Other"
mydat[, "Origin"] = as.factor(mydat[, "Origin"])
mydat[, "Dest"] = as.factor(mydat[, "Dest"])

#Group late, not late
mydat[(mydat$ArrDelay>0), "ArrDelay"] = "Late"
mydat[!(mydat$ArrDelay>0), "ArrDelay"] = "NotLate"
mydat[, "ArrDelay"] = as.factor(mydat[, "ArrDelay"])

###FITTING TREES!###
trainSet = mydat[1:150000, ]
testSet = mydat[-(1:150000),]
pred = clusterApplyLB(cl, 1:100, function(x){
	fit = randomForest(ArrDelay~., data = trainSet, ntree = 5, na.action = na.omit)
	predict(fit, testSet)
})
stopCluster(cl)
pred = do.call("rbind", pred)
votes = apply(pred, 2, function(x) which.max(table(x))[1])
comp = rbind(votes, as.numeric(testSet[,"ArrDelay"]))
sum(comp[1,]== comp[2,], na.rm = TRUE)

######################
###OTHER APPROACHES###
######################

##############
###IN SHELL###
###SED ONLY###
##############
mydat = clusterApplyLB(cl, files, function(x, n){
	lineCount = as.integer(strsplit(shell(paste("wc -l ", x, sep = ""), intern = TRUE), " ")[[1]][2])
	samp = sample(2:lineCount, n, replace = TRUE)
	lineCmd = paste("sed -n '", paste(samp, "p", sep = "", collapse = ";"), "' ", x, sep = "")
	read.csv(textConnection(shell(lineCmd, intern = TRUE)), header=FALSE)
}, n=100)
###################
#####IN SHELL######
###LOOP IN SHELL###
###################
mydat = clusterApplyLB(cl, files, function(x, n){
	lineCount = as.integer(strsplit(shell(paste("wc -l ", x, sep = ""), intern = TRUE), " ")[[1]][2])
	samp = sample(2:lineCount, n, replace = TRUE)
	lineCmd = paste("./looper.sh ", x, paste(samp, collapse = " "), sep = " ")
	read.csv(textConnection(shell(lineCmd, intern = TRUE)), header=FALSE)
}, n=100000)

##############
####IN SQL####
##############
library(RSQLite)
dr = dbDriver("SQLite")
con = dbConnect(dr, dbname = "airline")
cl = makeCluster(3, type = "FORK")
clusterSetRNGStream(cl)
files = list.files()[1:22]
mydat = clusterApplyLB(cl, files, function(x){
	countQuery = paste("SELECT count(*) FROM delays WHERE year = ", substr(x, 1, 4), ";", sep = "")
	countSQL = unlist(sqliteQuickSQL(con, countQuery))
	n = .1*countSQL
	samp = sample(1:countSQL, n, replace = TRUE)
	samp = sort(samp)
	datQuery = paste("SELECT * FROM delays WHERE year = ", substr(x, 1, 4), ";", sep = "")
	datSQL = dbSendQuery(con, datQuery)
	fetch(datSQL)[samp,] 
})
sqliteCloseResult(con)
stopCluster(cl)
mydat = do.call("rbind", mydat)
mydat = mydat[sample(1:nrow(mydat), nrow(mydat)),]
mydat = mydat[!is.na(mydat$ArrTime), ]
