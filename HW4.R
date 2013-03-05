setwd("~/Documents/STA 242/Assignment4/Data")
options(warn = -1)

#Goal: tally line counts for each "ORIGIN" airport, mean and standard deviation
#Compare in Shell(+R), only R, SQLite

strsplit(readLines("2000.csv", 1), ",")	##17 = ORIGIN, #15 = ARRDELAY
airports = c("LAX", "OAK", "SFO", "SMF")
filename = list.files(pattern = "[12]*.csv")
##############
#LAX: 4057452,5.89296459945798
#OAK: 1151897,5.00703448311785
#SFO: 2711958,7.77489732510607
#SMF: 806133,5.29944686546761
##############

##############
#####IN R#####
##############

countOriginsR = function(origin, B = 1000, files){
	total = structure(integer(length(origin)), names = origin)
	sub = sapply(files, function(x){
	con = file(x, "r")
	while(TRUE){
		ll = readLines(con, n=B)
		if(length(ll) == 0)
			break
		tmp = sapply(strsplit(ll, ","), `[[`, 17)
		subCounts = table(factor(tmp, origin))
		subCounts[is.na(subCounts)] = 0
		total = total + subCounts
	}
	close(con)
 	total})
 	apply(sub, 1, sum)
}

countOriginsR(airports, 1000, files = filename)

delayStatR1 = function(origin, B = 1000, files){
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
	list(mu = mean(delay, na.rm = TRUE), sd = sd(delay, na.rm = TRUE))
}

sapply(airports, delayStatR1, files = filename)

delayStatR2 = function(origin, B = 1000){
	countDelay = structure(integer(length(origin)), names = origin)
	sumDelay = structure(integer(length(origin)), names = origin)
	sumSqDelay = structure(integer(length(origin)), names = origin)
	con = pipe("LC_ALL=C cut -d',' -f15,17 [12]*.csv")
	while(TRUE){
		ll = readLines(con, n = B)
		if(length(ll) == 0)
			break
		tmp = sapply(strsplit(ll, ","), `[[`, 2)
		for(x in 1:length(origin)){
			dat = as.integer(sapply(strsplit(ll[tmp == origin[x]], ","), `[`, 1))
			countDelay[x] = countDelay[x] + length(dat[!is.na(dat)])
			sumDelay[x] = sumDelay[x] + sum(dat, na.rm = TRUE)
			sumSqDelay[x] = sumSqDelay[x] + sum(dat^2, na.rm = TRUE)
		}
	}
	close(con)
	rbind(countDelay, sumDelay, sumSqDelay)
}


myCalcsR = function(airports){
	temp = lapply(airports, delayStatR)
	sums = temp[[1]]
	for(i in 2:length(temp)){
		sums = sums + temp[[i]]}
	apply(sums, 2, function(x){
		mu = x[2]/x[1]
		sd = sqrt(x[3]/x[1] - mu)
	}
}
myCalcsR(airports)

##############
###IN SHELL###
##############
if(!exists("shell")){
	shell = system
}
countOriginsSh = function(origin){
	countsCmd = paste("LC_ALL=C cut -d',' -f17 [12]*.csv | egrep '(", paste(origin, sep = "", collapse = "|"), ")'| sort | uniq -c", sep = "")
	counts = shell(countsCmd, intern = TRUE)
	return(counts)
}
delayStatSh = function(origin){
	delayCmd = paste("awk -F',' '$17 == \"", paste(origin), "\" {print $0}' [12]*.csv | LC_ALL=C cut -d',' -f15", sep = "")
	stats = sapply(delayCmd, function(x){
		con = pipe(x)
		ll = readLines(con)
		mu = mean(as.numeric(ll), na.rm = TRUE)
		sd = sd(as.numeric(ll), na.rm = TRUE)
		close(con)
		list(mu = mu, sd = sd)
	})
	colnames(stats) = origin
	stats
}

countOriginsSh(airports)
delayStatSh(airports)

##############
####IN SQL####
##############
library(RSQLite)
dr = dbDriver("SQLite")
con = dbConnect(dr, dbname = "airline")
countSQL = sqliteQuickSQL(con, "SELECT Origin, count(*) FROM subDelays GROUP BY Origin ORDER BY Origin;")
statSQL = sqliteQuickSQL(con, "SELECT Origin, AVG(ArrDelay) as mu, (AVG(ArrDelay* ArrDelay) - AVG(ArrDelay)*AVG(ArrDelay)) as var FROM subDelays WHERE ArrDelay != 'NA' GROUP BY Origin ORDER BY Origin;")
sqliteCloseResult(con)
statSQL = cbind(statSQL, sd = sqrt(statSQL[,3]))