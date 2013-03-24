#! /usr/bin/env Rscript

input <- file( "stdin" , "r" )
lastKey <- ""

tempFile <- tempfile( pattern="airline-demo-" , fileext="csv" )
tempHandle <- file( tempFile , "w" )

while( TRUE ){
	currentLine <- readLines( input , n=1 )
	if( 0 == length( currentLine ) ){
		break
	}
	tuple <- unlist( strsplit( currentLine , "\t" ) )
	currentKey <- tuple[2]		##Airline Origin
	currentValue <- tuple[1]	##Arrival Delay
	if( ( currentKey != lastKey ) ){
		if( lastKey != "" ){
			close( tempHandle )
			bucket <- read.csv( tempFile , header = FALSE )
			result <- mean(bucket, na.rm = TRUE)
			cat( currentKey , "\t" , result , "\n" )
			tempHandle <- file( tempFile , "w" )
		}
		lastKey <- currentKey
	}
	cat( currentLine , "\n" , file=tempHandle )
}

close( tempHandle )
bucket <- read.csv( tempFile , header=FALSE )
result <- mean(bucket, na.rm = TRUE)
cat( currentKey , "\t" , result , "\n" )
unlink( tempFile )
close( input )
