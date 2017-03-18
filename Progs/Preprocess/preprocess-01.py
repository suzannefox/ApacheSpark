
import sys
import os
import re
from operator import add
import socket
from subprocess import call

from collections import defaultdict
from time import time
import datetime
import math

from pyspark import SparkConf, SparkContext

# ==========================================================================
# The purpose of this file is to examine all the input files for the 
# main run, and identify any big ones which might be anomalies or
# which will cause the main run to grind to a halt
# Examine humanly
# These files might reasonably be included in the run, but equally they 
# might be files like the Human Genome listing which are not relevant
# The program generates a python-format file in the format '...'filename','filename'...'
# which can be used in the main run to exclude these big files if desired
# ==========================================================================
stop_after = 0
batch_size = 100

# -------------------------------------------------------------
# Test for a line in the RDD containing the id and extract it
# -------------------------------------------------------------
def return_Id(RDD_file, each_file):
    Id_Types = ['[EBook', '[Etext', '[eBook' ]

    myId = ""
    got_Id = False
    for Id in Id_Types:
    	hasId = RDD_file.filter(lambda line: Id in line)
        if hasId.count() > 0:
            got_Id = True
            break

    # We don't have an Id
    if got_Id == False:
        return myId

    # We have an Id
    out = hasId.collect()

    line = out[0]
    offset_s = line.index(Id)
    line = line[offset_s:]
    line = re.sub('](.*)','',line)
    this_Id = re.sub('[^0-9]', '', line)

    return this_Id

# Need to generate 2 files as input which are listing using ls -lhR 
# of the full text files and the meta files for input to this prog

def size_m(line):
    # Convert multiple spaces to single
    line = re.sub(' +',' ',line)

    # Replace with commas
    line = re.sub(' ',',',line)

    # split into items, size is the 5th column
    s = line.split(',')

    intsize = re.sub('[^0-9.]','',s[4])

    if 'M' in s[4]:
        return float(intsize)
    else:
        return 0.0
        
# extract the filename and put it in a format I can use 
# as a list for the main run
def list_format(line):
    # Convert multiple spaces to single
    line = re.sub(' +',' ',line)

    # Replace with commas
    line = re.sub(' ',',',line)

    # split into items, filename is the 9th column
    s = line.split(',')
    return s[8] + "," + s[4]

def list_Excludes(sc, rdf_file_name):
    print ''
    print 'IDENTIFY DUPLICATE/NO META FILES FILES'

    data_path = '/data/extra/gutenberg/text-full'
    N = 0
    D = 0
    M = 0
    X = 0
    STOP = False
    batch_time = 0

    mode = "FULL"
    START = False
	
    if mode=="FULL":
        # use this to keep track of good files found so far
        # then we can check for duplicate ids against this file
         good = open("good",'w')
         good.close()

    # I ran this in sections due to speed issues on lewes
    if mode=="FULL":
        out = open("file_analysis.lst",'w')
        START = True
    else:
        out = open("file_analysis.lst",'a')
        START = False
	
	# This is the last file processed, so if I need to start this again
    # don't do anything until after this file
    last_file = '12890.txt'

# -------------------------------------------------------------
# Read the file from disk
# -------------------------------------------------------------
    RDD_csv_file = sc.textFile(rdf_file_name)
    print RDD_csv_file.count()

    FOLD = 0
    for root, dirs, files in os.walk(data_path):
        if STOP == True:
            break

        for each_file in files:
            file_name = root + '/' + each_file
			
            N +=1
            if stop_after > 0 and (N > stop_after):
                STOP = True
                break

            if last_file == each_file:
                START = True
                continue

            if START == False:
                continue
				

            # Let me know how it's going every batch_size files
            if N % batch_size == 0:
                batch_time = time() - batch_time
                print "{0} processed, {1} dups, batch time is {2}".format(N,D,batch_time)
                batch_time = time()

	    # -------------------------------------------------------------
            # Get the Id. If zero then none found	
	    # -------------------------------------------------------------
            RDD_file = sc.textFile(file_name)
            Id = return_Id(RDD_file, each_file)

            if Id == '':
                print >> out,"MISSING_ID,ID={0},{1},".format("-",each_file)
                M +=1
                continue

            # Check for duplicate
            RDD_good = sc.textFile("good")
            CheckString = "ID={0},".format(Id)
            Duplicate = RDD_good.filter(lambda myId: CheckString in myId).count()
            if Duplicate > 0:
                print >> out,"DUPLICATE,ID={0},{1},".format(Id,each_file)
                D +=1
                continue

            # Check for meta data
            Meta = RDD_csv_file.filter(lambda myId: CheckString in myId).count()
            if Meta == 0:
                print >> out,"NO_META,ID={0},{1},".format(Id,each_file)
                X +=1
                continue

            # write to good file
            if FOLD == 10:
                FOLD = 0
            good = open("good",'a')
            print >> good,"ID={0}, {1}".format(Id, each_file)
            good.close()
            print >> out,"GOOD, ID={0},{1},{2},".format(Id, each_file, FOLD)
            FOLD +=1
	
	print "checked {0} files, {1} duplicates, {2} no id, {3} no meta".format(N, D, M, X)

def list_Big(sc, file_big):
    print ''
    print 'IDENTIFY BIG FILES'

    big = open(file_big,'w')
    RDD_file = sc.textFile(txt_file_name)

    # get lines with .txt in
    RDD_file = RDD_file.filter(lambda line: '.txt' in line)
    print "TOTAL .TXT files in directories is ",RDD_file.count()
	
    # get lines where size is in M and > 2.5
    RDD_file = RDD_file.filter(lambda line: size_m(line) > 2.5)

	# Give me the filename
    RDD_file = RDD_file.map(lambda line: list_format(line))

    lines = RDD_file.collect()
    print "There are {0} files > 2.5M".format(RDD_file.count())

    for line in lines:
        print >> big, "TOOBIG,{0},".format(line)

    big.close()

def do_analysis(sc, in_file):
    files = sc.textFile(in_file)

    types = files.map(lambda line: (line.split(",")[0])) \
				 .map(lambda word: (word, 1)).reduceByKey(add)
    out = types.collect()

    print "ANALYSIS OF first item of input file"
    for x in out:
       print x
	   
# ===============================================================================
# The main function
# ===============================================================================
if __name__ == "__main__":
    if len(sys.argv) !=3:
        print >> sys.stderr,"Supply arguments - MODE filename"
        exit(-1)

    MODE = sys.argv[1]
    # If MODE="BIG" then process a file created from ls -lhR /data/extra ...
	# and identify files > than the specified size and write them
    # into a file called exclude_big.lst
    if MODE.upper() == "BIG":
        file_big = "exclude_big.lst"
        txt_file_name = sys.argv[2]

    # If MODE="EXCLUDES" then process all the files in the directory
    # Identify duplicates, those with no Id in the header, and those with
	# no meta data. meta data is found in a XML.csv file created as the output 
    # from running CW-part2.py and is the 2nd arguement in this case
    elif MODE.upper() == "EXCLUDES":
        rdf_file_name = sys.argv[2]

    elif MODE.upper() == "ANALYSIS":
        in_file = sys.argv[2]

    else:
        print "arguements are MODE (EXCLUDES, BIG, ANALYSIS) filename"	
        exit(-1)

    conf = (SparkConf().setMaster("local[2]").setAppName("File Sizes"))
    sc = SparkContext(conf = conf)

    if MODE.upper() == "EXCLUDES":
        list_Excludes(sc, rdf_file_name)
        print "Finished."
		
    if MODE.upper() == "BIG":
        list_Big(sc, file_big)
        print "You can use {0} as an input for the main run to exclude super-massive files".format(file_big)

    if MODE.upper() == 'ANALYSIS':
        do_analysis(sc, in_file)

    sc.stop() # Disconnect from Spark
    exit(-1)


