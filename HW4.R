setwd("~/Documents/STA 242/Assignment4/Data")

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
	list(mu = mean(delay, na.rm = TRUE), sd = sd(delay, na.rm = TRUE))
}

sapply(airports, delayStatR, files = filename)

##############
###IN SHELL###
##############
##ADD THIS?
##tar -jxvf filename.tar.bz2


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
	con = pipe(delayStatSh)
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
sqliteQuickSQL(
create table delays (
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier varchar(5),
  FlightNum int,
  TailNum varchar(8),
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin varchar(3),
  Dest varchar(3),
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
);

.separator ,
.import 1987.csv delays
.import 1988.csv delays
.import 1989.csv delays
.import 1990.csv delays
.import 1991.csv delays
.import 1992.csv delays
.import 1993.csv delays
.import 1994.csv delays
.import 1995.csv delays
.import 1996.csv delays
.import 1997.csv delays
.import 1998.csv delays
.import 1999.csv delays
.import 2000.csv delays
.import 2001.csv delays
.import 2002.csv delays
.import 2003.csv delays
.import 2004.csv delays
.import 2005.csv delays
.import 2006.csv delays
.import 2007.csv delays
.import 2008.csv delays

delete from delays where typeof(year) == "text";

create index year on delays(year);
create index date on delays(year, month, dayofmonth);
create index origin on delays(origin);
create index dest on delays(dest);

SELECT 
	count(*), AVG(ArrDelay), STDEV(ArrDelay) 
FROM delays 
WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') 
GROUP BY Origin )
sqliteCloseResult()