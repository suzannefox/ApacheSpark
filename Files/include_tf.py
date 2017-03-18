# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 06:53:41 2014

@author: macuser
"""

from subprocess import call
from operator import add
from time import time
import datetime
import socket
import math
import sys
import os
import re

# list of function words
stopwords = ['in','on','of','out','by','from','to','over','under','the','a','when', \
             'where','what','who','whom','you','thou','go','must','i','me','my','myself']

import return_Exclude
import return_Header
import return_Footer

# ============================================
# Return a normalised TF for each file
# ============================================
def return_tf_normalized(RDD_file):
    wordCounts = RDD_file.flatMap(lambda line: re.split('\W+',line)) \
					  .map(lambda word: word.lower()) \
					  .map(lambda word: re.sub('\.','',word)) \
					  .map(lambda word: re.sub(',','',word)) \
					  .map(lambda word: re.sub(';','',word)) \
					  .map(lambda word: re.sub('_','',word)) \
                              .filter(lambda word: re.sub('[0-9.]','',word) != 'th') \
                              .filter(lambda word: re.sub('[0-9.]','',word) != 'st') \
					  .filter(lambda word: len(word) > 2) \
                              .filter(lambda word: word.isdigit() == False) \
					  .map(lambda word: (word, 1)) \
					  .reduceByKey(lambda word, count: word + count)

# find the most frequent word
    max = wordCounts.values().reduce(lambda a, b: a if (a > b) else b)

# normalise the counts
    wordCounts = wordCounts.map(lambda (word, count): (word, float(count)/max))

    return wordCounts

# ============================================
# Create all the .TFS
# ============================================
def create_tf(data_path, sc, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics):
    diagnostics = False
    start_time = time()
    batch_time = time()
    file_counter = 0
    STOP = False
    FOLD = 0
	
    print >> diag_file,"create_tf : Run started at ",datetime.datetime.now()

    # Load the file analysis
    RDD_Files = sc.textFile('file_analysis.lst',10)
    RDD_Files.cache()
    # Load the subjects, so I can get the top 10 subjests per the set of files that I use
    # later on for the classifier training
    RDD_Meta = sc.textFile('XML.csv')
    RDD_Meta.cache()

    # list .TFs created
    tf = open('sparkrun_tf.csv','w')

    if diagnostics == True:
        print "data path ",data_path
        
    for root, dirs, files in os.walk(data_path):
        if STOP == True:
		    break
			
        for each_file in files:
            if not '.txt' in each_file:
                continue

            # Is this a good file ?
            search = ',' + each_file + ','
            RDD_Good = RDD_Files.filter(lambda line: search in line)
            good_line = RDD_Good.collect()
            good_line = good_line[0].split(',')

            if RDD_Good.count() != 1:
                continue

            if good_line[0] != 'GOOD':
                continue

            Id = re.sub('[^0-9]','',good_line[1])
			
            # Do I want to exclude it ?
            if return_Exclude.Exclude(each_file) == True:
                    continue
								
            file_name = root + '/' + each_file
			
            file_counter +=1
            if stop_after > 0 and (file_counter > stop_after):
                STOP = True
                file_counter -=1
                break

            # Let me know how it's going every batch_size files
            if file_counter % batch_size == 0:
                batch_time = time() - batch_time
                print "{0} processed, batch time is {1}".format(file_counter, batch_time)
                print >> runtimes_file,"{0} processed, batch time is {1}".format(file_counter, batch_time)
                batch_time = time()

	    # -------------------------------------------------------------
	    # Read the file from disk
        # output it so I can strip off the header and footer then re-RDD
		# it. Couldn't figure a way to do this in Spark as it has no concept
		# of line numbers, but it seemed to work quick enough
	    # -------------------------------------------------------------
            RDD_file = sc.textFile(file_name)
            RDD_output = RDD_file.collect()

	    # -------------------------------------------------------------
		# Get the Header string to match. If blank then none found	
	    # -------------------------------------------------------------
            Header = return_Header.Header(RDD_file, diag_file, file_name, report_diagnostics)
            if Header == '':
                continue

            # Find the line no which contains the header
            line_no = 0
            line_header = -1

            for line in RDD_output:
                line_no +=1
                if re.search(Header, line) != None:
                    break

            line_header = line_no

	    # -------------------------------------------------------------
            # Look for a footer string to match. If blank then none found	
	    # -------------------------------------------------------------
            Footer = return_Footer.Footer(RDD_file, diag_file, file_name, report_diagnostics)
            line_footer = -1
            if Footer != '':
                line_no = 0
                
                for line in RDD_output:
                    line_no +=1
                    if re.search(Footer, line) != None:
					    break
                
                line_footer = line_no - 1

	    # -------------------------------------------------------------
            # Strip out the important bits of the file
            # Create a new RDD to make the bag of words from
	    # -------------------------------------------------------------			
            if line_footer > 0:
                RDD_output = RDD_output[line_header:line_footer]
            else:
                RDD_output = RDD_output[line_header:]

            # -------------------------------------------------------------
	    # Load stripped file into a new RDD and filter out blank lines
	    # -------------------------------------------------------------
            RDD_strippedfile = sc.parallelize(RDD_output)
            RDD_strippedfile = RDD_strippedfile.filter(lambda line: len(line) > 0)

            if report_diagnostics == True :
                print >> diag_file, each_file,", BEFORE ",RDD_file.count(),\
								  " STRIPPED ",RDD_strippedfile.count(),\
                                              ", HEADER at ",line_header,", FOOTER at",line_footer,

	    # -------------------------------------------------------------
	    # Create a (word, count) RDD
	    # -------------------------------------------------------------
            RDD_TF = return_tf_normalized(RDD_strippedfile)
            if diagnostics == True:
                print "File {0} has {1} ".format(each_file, RDD_TF.count())
                
            # Get the subjects so we can calculate top 10 for the TFs we create later on
            search = "ID={0},".format(Id)
            meta_data = RDD_Meta.filter(lambda line: search in line).collect()

            # Save to disk
            save_name = Id + '.tf'
            call(["rm", "-rf", 'tfs/' + save_name])
            try:
                RDD_TF.saveAsPickleFile('tfs/' + save_name)
                print >> tf,"{0},{1},{2}".format(save_name, FOLD, meta_data[0])
            except:
                print "file exists ",'tfs/' + save_name

            FOLD +=1
            if FOLD == 10:
                FOLD = 0

    tf.close()				
    # Finished iterating the files
    print "FINISHED, {0} .TF files created, see sparkrun_tf.csv.".format(file_counter)

