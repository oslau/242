setwd("~/Documents/STA 242/Assignment4/")

#Goal: tally line counts for each "ORIGIN" airport, mean and standard deviation
#Compare in Shell(+R), only R, SQLite

strsplit(readLines("2000.csv", 1), ",")	##17 = ORIGIN, #15 = ARRDELAY
airports = c("LAX", "OAK", "SFO", "SMF")
filename = paste(1987:2008, ".csv", sep = "")

##############
#####IN R#####
##############

countOriginsR = function(origin, B = 1000, files){
	con = file(files, "r")
	total = structure(integer(length(origin)), names = origin)
	while(TRUE){
		ll = readLines(con, n=B)
		if(length(ll) == 0)
			break
	tmp = sapply(strsplit(ll, ","), `[[`, 17)
	subCounts = table(factor(tmp, origin))
	subCounts[is.na(subCounts)] = 0 ## or, convert tmp[origin] to factor: )
	total = total + subCounts
	}
 total
}

countOriginsR(airports, 1000, files = filename)

delayStatR = function(origin, B = 1000, files){
	con = file(files, "r")
	delay = integer()
	while(TRUE){
		ll = readLines(con, n = B)
		if(length(ll) == 0)
			break
		tmp = sapply(strsplit(ll, ","), `[[`, 17)
		dat = as.integer(sapply(strsplit(ll[tmp == origin], ","), `[`, 15))
		delay = c(delay, dat)
	}
	list(mean(delay, na.rm = TRUE), sd(delay, na.rm = TRUE))
}

##############
###IN SHELL###
##############
if(!exists("shell")){
	shell = system
}
countOriginsSh = function(origin){
	countsCmd = paste("cut -d',' -f17 [12]*.csv | egrep '(", paste(origin, sep = "", collapse = "|"), ")'| sort | uniq -c", sep = "")
	counts = shell(countsCmd, intern = TRUE)
	return(counts)
}
delayStatSh = function(origin){
	delayCmd = paste("awk -F',' '$17 == \"", paste(origin), "\" {print $0}' [12]*.csv | cut -d',' -f15", sep = "")
	stats = sapply(delayCmd, function(x){
		delays = as.numeric(shell(x, intern = TRUE))[-1]
		mu = mean(delays, na.rm = TRUE)
		sd = sd(delays, na.rm = TRUE)
		list(mu = mu, sd = sd)
	})
colnames(stats) = origin
}

countOriginsSh(airports)
delayStatSh(airports)

##############
####IN SQL####
##############
library(RSQLite)
dr = dbDriver("SQLite")
con = dbConnect(dr, dbname = "file.sqlite")
sqliteQuickSQL(SELECT count(*), AVG(ArrDelay), STDEV(ArrDelay) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY ORIGIN )
sqliteCloseResult()