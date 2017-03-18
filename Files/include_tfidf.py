# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 06:45:25 2014

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

# ============================================
# Create a vsize hash vector
# ============================================
def return_HashVector(sc, RDD_TFIDF, vsize):
        # Create hash vector
        # I know I should do this from an RDD with a lambda,
        # but time is tight and my RDDs are a different structure
        # to the lab4 ones, and I just can't figure the syntax
        output = RDD_TFIDF.collect()
        vec = [0] * vsize
        for wcpairs in output:
            i = hash(wcpairs[0]) % vsize
            vec[i] = vec[i] + wcpairs[1]
        
        RDD_HASH = sc.parallelize(vec)
        return RDD_HASH
        
# ============================================
# Create the TF.IDF
# ============================================
def create_tfidf(sc, RDD_IDF, diag_file, runtimes_file, stop_after, batch_size, report_diagnostics):
    start_time = time()
    batch_time = time()
    file_counter = 0
    report_diagnostics = False

    start_time = time()
    txt_files = open('sparkrun_idf.csv')
    
    # write a csv file of the results for various HashSize
    rpt = open('4d_stats.csv','a')
    print >> rpt,"{0},{1},{2},{3},{4}".format('HashSize', \
                                              'File', \
                                              'Fold', \
                                              'tf words', \
                                              'Time taken')

    for txt_line in txt_files:
        file_counter +=1

        start_tf = time()
        # get file and fold from the csv driver file
        txt_line = txt_line.split(',')
        file_tf = re.sub("\'","",txt_line[0])
        file_tf = re.sub("\[","",file_tf)
        FOLD = re.sub("\'","",txt_line[1])
        FOLD = re.sub(' ','',FOLD)

        # load the picked TF
        RDD_TF = sc.pickleFile('tfs/' + file_tf)
        
        # join with the IDF
        RDD_TF = RDD_TF.join(RDD_IDF)
        if report_diagnostics:
            output = RDD_TF.take(10)
            print "JOINED ", output

        # create the TF.IDF
        RDD_TFIDF = RDD_TF.flatMap(lambda (term, (tf, idf)): [(term, (tf * idf))])
        if report_diagnostics:
            output = RDD_TFIDF.take(10)
            print ""
            print "MULTIPLIED ",output
            print file_tf," count ",output

        # Create the 10,000 hash vector
        RDD_HASH = return_HashVector(sc, RDD_TFIDF, 10000)
        if report_diagnostics:
            output = RDD_HASH.take(10)
            print ""
            print "VECTOR format ",output

        finish_tf = time()
        # collect stats            
        print >> rpt,"{0},{1},{2},{3},{4}".format(10000, \
                                                  file_tf, \
                                                  FOLD, \
                                                  RDD_TF.count(), \
                                                  finish_tf - start_tf)


        # save the hash vector to disk in fold paths    
        save_name = '{0}/{1}idf'.format(FOLD, file_tf)
        if report_diagnostics == True:
            print file_tf, FOLD, int(FOLD)

        call(["rm", "-rf", save_name])
        try:
            RDD_HASH.saveAsPickleFile(save_name)
        except:
            print "file exists ",save_name

        # Let me know how it's going every batch_size files
        if file_counter % batch_size == 0:
           batch_time = time() - batch_time
           print "{0} processed, batch time is {1:.0f}".format(file_counter, batch_time)
           print >> runtimes_file,"{0} processed, batch time is {1}".format(file_counter, batch_time)
           batch_time = time()

    rpt.close()    
    print "FINISHED,time taken {1:.0f} : {0} TF.IDF files created.".format(file_counter, time()-start_time)
        
