#
# SUZANNE FOX - Big Data Coursework - November 2014
#

from subprocess import call
from operator import add
from time import time
import numpy as np
import datetime
import socket
import math
import sys
import os
import re

from pyspark import SparkConf, SparkContext

# ===========================================================================
# Pre-processing - see report for details
# ===========================================================================
#
# NOTE : I started this work quite soon after the instructions were released and so
#        followed the instruction to use textFile, not wholetextFile. This makes quite
#        a difference in several places in the code because the RDDs contain lines
#        not file, content pairs. For instance in the lab sessions you describe a 
#        much more effective method of stripping the header than the one I've used, 
#        but it relies on having the content available. 
# 
# NOTE : Quite late in the day I read about Spark accumulators and how I could
#        use the foreach construction to process across tuples, and I could have
#        used this had I known about it earlier.
#
# Creates TFs, IDF, TF.IDF in separate stages. The parameter stop_after allows the 
# number of files processed to be user defined. 
#
# Stage 1 - TF Creation
# - iterate through .txt files on disk
# - look up in file_analysis.lst, if GOOD get Id from file_analysis.lst
#   otherwise go on to next file
# - if they are in the Exclude list because they are too big go to next file
# - split off Header, Footer (if found). remove punctuation, force to all lower
#   case so capitalised words at the start of sentances are not duplicated
#   would put stopwords in this part, but ran out of time
# - create RDD of (word, count) pairs, normalise, save to disk
# - look up meta data from XML.csv
# - Create a FOLD number from 0 to 9 to tag each file. This will
#   be used at Stage 3 to split files into 10 Folds for classifier training 
# - write a record to tf.lst with file_name, FOLD, ID, Subjects
#
# Stage 2 - IDF Creation
# - iterate through tf.lst created at Stage 1 for stop_after number of files
# - get (word, counts) from .TF, change to (word, 1) to give document counts
# - accumulate. Couldn't get this bit working for >~ 100 .TFs
#   without crashing Spark.
# - write record (with subjects) from tf.lst to idf.csv. if I want to do
#   an analysis on a subset then I have the subjects just for that subset of files
# - create IDF from TFs specified
#
# Stage 3 - TF.IDF
# - iterate through idf.csv created at Stage 2. All files from Stage 2 have
#   to be included or else the IDF makes no sense, so stop_after is not relevant
# - Calculate TF.IDF
# - write to Fold directory specified
#
# USAGE -> spark-submit CW-Part1.py MODE <optional> number_to_process
#          MODE = TF, IDF, TF.IDF or ALL
# ===========================================================================
 
# =====================================================================================
# Define some global variables which I'll use to control how the code behaves. 
# During the development phase I want to identify and report various patterns in the data 
# until I have learned the structure. For the final run I'll turn these diagnostics off
# to speed up the processing time. I can use these variables to trade off knowledge
# for speed
# ===================================================================================== 
stop_after = 20            # only process this number of files. If zero then do everything
                           # This can be overridden by an optional sys.argv parameter if supplied 
batch_size = 10            # report to the screen after this number of files has been processed
report_diagnostics = False
diag_file = open('diagnostics.log','w')
runtimes_file = open('runtimes.csv','a')

import include_tfidf as TFIDF
import include_idf as IDF
import include_tf as TF
import include_Models as MODELS

# =====================================================================
# It's The Main Thing
# =====================================================================
if __name__ == "__main__":    
    if len(sys.argv) < 2:
        print >> sys.stderr,"Supply arguments - MODE <optional> number_to_process"
        exit(-1)

    # I can run this from home (PC or mac) or lewes, started with the PC at home but
    # then mac proved easier and more similar to environment on Lewes
    MyComputer=socket.gethostname()    
    if MyComputer[0:4]=='Suza':
        data_path = "C:/Users/Public/Documents/MSc-DataScience/INM432/CW/"
        
    elif MyComputer[0:4].upper()=='MACS':
        data_path = "/Users/macuser/Documents/spark/text-part/"
        
    else:
        data_path = "/data/extra/gutenberg/text-part/"
    	
    # ==================================================================
    # Arg 2 is MODE, you can run the whole TF.IDF creation in one go
    #                or in stages. Each stage creates a .CSV which 
    #                the next stage uses to know what to do
    #                READ include_comments.py for full details
    # Arg 3 is stop_after, you can chose how many .txt files to process
    #                so easy to adjust the scope of the run
    #                this arg doesn't apply to TF.IDF runs because
    #                you have to create the TF.IDFs for whatever files
    #                went into the IDF or it doesn't make any sense
    # ==================================================================
    MODE = sys.argv[1].upper()
    if len(sys.argv) == 3:
        stop_after = int(sys.argv[2])
	
    MODES = ['ALL','TF','IDF','TF.IDF','CALC.IDF','NAIVE-BAYES','DECISION-TREES','LOGISTIC-REGRESSION']
    for mode in MODES:
        if mode == MODE:
            break
        
    if not mode == MODE:
        print ""
        print "No such MODE parameter as {0}, ending.".format(MODE)
        print ""
        exit(-1)

    print ""
    print "=================================================================="
    print "Main : Starting run at : ", datetime.datetime.now()
    print "Running on ",MyComputer
    print "Getting data from ",data_path
    print "Processing {0} files in batches of {1}".format(stop_after, batch_size)
    print >> runtimes_file
    print >> runtimes_file,"NEW RUN, STARTED AT {0}, STOP_AFTER = {1}".format(datetime.datetime.now(),stop_after)
    print ""

	# Connect to Spark
    config = SparkConf().setMaster('local[2]').set('spark.executor.memory', '4g')
    sc = SparkContext(conf = config, appName = 'SFOX - Coursework INM432')
        
    # ===============================================================================
    # Part 1 - Reading and Preparing text files
    # ===============================================================================
    
    # Part 1d - create TF
    if MODE == 'TF':
        print ""
        print "CREATING TFs"
        print >> runtimes_file,"CREATING TF"
        TF.create_tf(data_path, sc, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)

    # Part 1e - create IDF
    if MODE == 'IDF':
        print ""
        print "CREATING IDF"
        print >> runtimes_file,"CREATING IDF"
        RDD_IDF = sc.parallelize([])
        RDD_IDF = IDF.create_idf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)
        # Finished processing
        print "Mapping ..."
        N = 50.0
        start_map = time()
        RDD_IDF = sc.pickleFile('IDF_RAW.RDD')
#        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, ( N / count)))
        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, np.log( N / count)))
        save_name = 'IDF.RDD'
        call(["rm", "-rf", save_name])
        RDD_IDF.saveAsPickleFile(save_name)
        finish_map = time()
        print "Mapped, took ",finish_map - start_map
    
    if MODE == 'CALC.IDF':
        # Finished processing
        print "Mapping ..."
        N = 50.0
        start_map = time()
        RDD_IDF = sc.pickleFile('IDF_RAW.RDD')
#        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, ( N / count)))
        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, np.log( N / count)))
        save_name = 'IDF.RDD'
        call(["rm", "-rf", save_name])
        RDD_IDF.saveAsPickleFile(save_name)
        finish_map = time()
        print "Mapped, took ",finish_map - start_map

    # Part 1f - create TF.IDF
    if MODE == 'TF.IDF':
        print ""
        print "CREATING TF.IDFs"
        print >> runtimes_file,"CREATING TF.IDF"
        RDD_IDF = sc.pickleFile('IDF.RDD')
        TFIDF.create_tfidf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)

    # Part 1d/e/f - Do the whole thing for 'stop_after' files
    if MODE == 'ALL':
        print ""
        print "CREATING TFs"
        print >> runtimes_file,"CREATING TFs"
        TF.create_tf(data_path, sc, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)
        print ""
        print "CREATING IDF"
        print >> runtimes_file,"CREATING IDF"
        RDD_IDF = sc.parallelize([])
        RDD_IDF = IDF.create_idf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)
        # Finished processing
        print "Mapping ..."
        N = 50.0
        start_map = time()
        RDD_IDF = sc.pickleFile('IDF_RAW.RDD')
#        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, ( N / count)))
        RDD_IDF = RDD_IDF.map(lambda (word, count): (word, np.log( N / count)))
        save_name = 'IDF.RDD'
        call(["rm", "-rf", save_name])
        RDD_IDF.saveAsPickleFile(save_name)
        finish_map = time()
        print "Mapped, took ",finish_map - start_map
        print ""
        print "CREATING TF.IDFs"
        print >> runtimes_file,"CREATING TF.IDF"
        RDD_IDF = sc.pickleFile('IDF.RDD')
        TFIDF.create_tfidf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics)

    # ===============================================================================
    # Part 2 - Reading and Preparing Meta data from XML
    # ===============================================================================
    # Read the comments in include_Comments.py for full explantion
    # This is in a separate .py file which happens as part of the preprocessing stages

    # ===============================================================================
    # Part 3 - Training Classifiers
    # ===============================================================================
    if MODE == 'NAIVE-BAYES':
        MODELS.model_NaiveBayes(sc, diag_file, runtimes_file, report_diagnostics)
        
    HashSize = 10000
    if MODE == 'DECISION-TREES':
        MODELS.model_DecisionTrees(sc, HashSize, diag_file, runtimes_file, report_diagnostics)
        
    if MODE == 'LOGISTIC-REGRESSION':
        MODELS.model_LogisticRegression(sc, HashSize, diag_file, runtimes_file, report_diagnostics)

    # close spark
    sc.stop()
    diag_file.close()
    runtimes_file.close()
