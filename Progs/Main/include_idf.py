# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 06:52:10 2014

@author: macuser
"""
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

# ============================================
# Create the .IDF
# ============================================
def create_idf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics):
    start_time = time()
    batch_time = time()
    report_diagnostics = True
    
    print >> diag_file,"create_idf : Run started at ",datetime.datetime.now()
    
    # write to this file which will form the input for the TF.IDF run
    idf_csv = open('sparkrun_idf.csv','w')

    # Number of documents. Important to use 0.0 not 0 here
    N = 0.0

    # These are the files that were created in the TF run
    txt_files = open('sparkrun_tf.csv')
    
    # Some temp RDDs for diagnostics
    RDD_1 = sc.parallelize([])
    RDD_2 = sc.parallelize([])
    RDD_already = sc.parallelize([])

    # iterate through the files
    for txt_line in txt_files:
        N +=1

        if stop_after > 0 and (N > stop_after):
            N -=1
            break

        # Let me know how it's going every batch_size files
        if N % batch_size == 0:
           batch_time = time() - batch_time
           save_name = 'IDF.RDD'
           call(["rm", "-rf", save_name])
           
           # Try this to speed things up every batch, think it's to do with java Garbage Collection ?
           # serialising like this makes a difference to run times, but the merging gets slow after 
           # about 45 files no matter what I do
           RDD_IDF.saveAsPickleFile(save_name)
           RDD_IDF = sc.pickleFile('IDF.RDD')
           RDD_IDF.coalesce(100, shuffle=True)
           print "{0} processed, batch time is {1:.0f}, vocab {2}".format(N, batch_time, RDD_IDF.count())
           print >> runtimes_file,"{0} processed, batch time is {1:.0f}".format(N, batch_time)
           batch_time = time()
        
        # Record that this file was used for the IDF
        # so I know what files and set of subjects has gone into the 
        # IDF calculation. I'll use this to drive the TF.IDF creation
        # and also later in calculating the top 10 subjects, and identifying
        # whether a file is classified as a subject when training the
        # model classifiers
        print >> idf_csv, re.sub('\n','',txt_line) + ','

        # get the tf file
        txt_line = txt_line.split(',')
        file_tf = re.sub('[^0-9]','',txt_line[0]) + '.tf'

        RDD_TF = sc.pickleFile('tfs/' + file_tf)
        RDD_Words = RDD_TF.map(lambda (word, count): (word, 1))

        # get the stats of what's going on, numbers before the merge
        if report_diagnostics:
            RDD_1 = RDD_Words.map(lambda (word, count): word)
            RDD_2 = RDD_IDF.map(lambda (word, count): word)
            RDD_already = RDD_1.intersection(RDD_2)

        # Add it to the IDF 
        start_idf = time()
        if N == 1:
            RDD_IDF = RDD_Words
        else:
            # Merge the incoming TF words into the IDF and count
            IDF_merge = (RDD_IDF + RDD_Words).combineByKey(lambda value: (value, 1),
                                                           lambda x, value: (x[0] + value, x[1] + 1),
                                                           lambda x, y: (x[0] + y[0], x[1] + y[1]))

            RDD_IDF = IDF_merge.map(lambda (label, (value_sum, count)): (label, value_sum))

        # report details of each iteration to look for anomalous files
        finish_idf = time()

        if N > 1 & report_diagnostics == True:
                print "TFs Added {0}, {1}, word # {2}, in IDF {3}, new {4}, vocab now {5}, time taken {6:.2f}" \
                      .format(N, \
                             'tfs/'+file_tf, \
                              RDD_1.count(), \
                              RDD_already.count(), \
                              RDD_Words.count() - RDD_already.count(), \
                              RDD_IDF.count(), \
                              finish_idf - start_idf)
                              
        print >> runtimes_file,"TFs added {0}, {1}, vocab {2}, time taken {3}".format(N, file_tf, RDD_IDF.count(), finish_idf - start_idf)

    # Save to disk. Keep a copy of the pre-divided IDF because I'm confused about whther to
    # divide by N or log N, so I can just pick up this file and try both ways
    print  "Saving ..."
    start_save = time()
    save_name = 'IDF_RAW.RDD'
    call(["rm", "-rf", save_name])
    RDD_IDF.saveAsPickleFile(save_name)
    done_save = time()

    idf_csv.close()
    txt_files.close()
        
    # Finished processing
    print "Mapping ..."
    start_map = time()
    RDD_IDF = sc.pickleFile('IDF_RAW.RDD')
    RDD_IDF = RDD_IDF.map(lambda (word, count): (word, np.log( N / count)))
    save_name = 'IDF.RDD'
    call(["rm", "-rf", save_name])
    RDD_IDF.saveAsPickleFile(save_name)
    finish_map = time()
    print "Mapped, took ",finish_map - start_map

    if report_diagnostics:
        print RDD_IDF.take(10)

    print "Time to save ",done_save - start_save
    print >> runtimes_file,"IDF Save time,",done_save - start_save
    print "FINISHED, {0} file included in IDF, see sparkrun_idf.csv.".format(N)
    print "Total run time : ",time() - start_time
