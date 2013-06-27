#!/bin/bash

#-------------------------------
#  cs292 - homework 6
#
#  Gaurav Gupta
#-------------------------------


#--------------------------------------------------------------------
#  Page Rank Algorithm
#
#  Useage: 
#      ./run_pagerank.sh <in> <out> <discount> <iterations>
#  Where <in> is the input file or directory on hadoop,
#        <out> is the directory storing the complete rankings
#        <iternations> is the number of repititions of the algorithm
#  Runs the default settings if there are no arguments
#--------------------------------------------------------------------


if [ $# -eq 3 ]; then
    #use the command line arguments
    hadoop jar ./runpagerank.jar cs292.hw6.RunPageRank \
	$1 $2 $3
    echo ""
    echo "Top 10 page ranks for "$1
    hadoop fs -cat $2/part-00000 | head
else
#default parameters
    ITERATIONS="1"
    SCOWIKI="scowiki-20090929-one-page-per-line"
    AFWIKI="afwiki-20091002-one-page-per-line"
    OUTFILE="pagerank-out"
    
# 1st run on,  scowiki file
    hadoop jar ./runpagerank.jar cs292.hw5.RunPageRank \
	$SCOWIKI $OUTFILE $ITERATIONS
    echo ""
    echo "Top 10 page ranks for "$SCOWIKI
    hadoop fs -cat $OUTFILE/part-00000 | head
    echo ""

# 2nd run, on afwiki file
    INFILE="afwiki-20091002-one-page-per-line"
    hadoop jar ./runpagerank.jar cs292.hw5.RunPageRank \
	$AFWIKI $OUTFILE $ITERATIONS
    echo ""
    echo "Top 10 page ranks for "$AFWIKI
    hadoop fs -cat $OUTFILE/part-00000 | head
    echo ""
fi
