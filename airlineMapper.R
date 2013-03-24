#! /usr/bin/env Rscript

input <- file( "stdin" , "r" )

while( TRUE ){

	currentLine <- readLines( input , n=1 )
	if( 0 == length( currentLine ) ){
		break
	}

	currentFields <- unlist(strsplit( currentLine , "," ) )

	result <- paste(
		currentFields[15] ,	##delay, value
		currentFields[17] ,	##origin, key
		sep="\t"
	) 

	cat( result , "\n" ) 

}

close( input )