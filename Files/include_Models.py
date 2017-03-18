# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 11:10:47 2014

@author: macuser
"""
from collections import defaultdict
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
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.classification import LogisticRegressionWithSGD

# =====================================================================
# The TF.IDF files created in Part 1 will be logged in the csv file
# sparkrun_idf.csv, along with the subjects from the metadata
# to find the top 10 for the corpus of analysed files, I just 
# need to analyse the CSV file. If I'm using the TF files for the 
# decision trees then I can get the subjects from sparkrun_tf.csv
# =====================================================================
def get_top10_Subjects(sc, mode, diag_file, runtimes_file, report_diagnostics):
    report_diagnostics = False # overwrite locally if I want just this reported
    
    # Get the subjects from the TF or IDF run report
    # Want the idfs for Naive Bayes, but tfs for decision trees
    if mode =='tf':
        RDD_Subjects = sc.textFile('sparkrun_tf.csv')
    else:
        RDD_Subjects = sc.textFile('sparkrun_idf.csv')

    RDD_Subjects.cache()
    
    RDD_TopSubjects = RDD_Subjects.flatMap(lambda line: line.split(',')[3:])
    RDD_TopSubjects = RDD_TopSubjects.filter(lambda subject: len(subject) > 0)
    RDD_TopSubjects = RDD_TopSubjects.map(lambda subject: (subject, 1))
    RDD_TopSubjects = RDD_TopSubjects.reduceByKey(add)
    # Toggle tuples so count is the key which makes sort easy
    RDD_TopSubjects = RDD_TopSubjects.map(lambda (word, count): (count, word))
    # count them and sortby key descending to get top 10    
    RDD_TopSubjects = RDD_TopSubjects.sortByKey(False)
    Top_Subjects = RDD_TopSubjects.take(10)       
    
    if report_diagnostics:
        print "TOP 10 Subjects are "
        print Top_Subjects
        print ""

    return Top_Subjects

# ============================================
# Create a vsize hash vector
# ============================================
def return_HashVector(sc, RDD_TF, vsize):
        # Create hash vector
        # I know I should do this from an RDD with a lambda,
        # but time is tight and my RDDs are a different structure
        # to the lab4 ones, and I just can't figure the syntax
        output = RDD_TF.collect()
        vec = [0] * vsize
        for wcpairs in output:
            i = hash(wcpairs[0]) % vsize
            vec[i] = vec[i] + wcpairs[1]
        
        RDD_HASH = sc.parallelize(vec)
        return RDD_HASH

# =====================================================================
# This function returns an RDD of the (word, count) TFs from the folds 
# specified with a label of 1.0 if the file contains the subject
# and 0.0 otherwise
# =====================================================================
def return_data(sc, Subject, HashSize, Folds, diag_file, runtimes_file, report_diagnostics):
    report_diagnostics = False
    # Very simple toy data so I know what I'm aiming for
#    trainData = [(0.0, [0.0, 3.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (1.0, [2.0, 1.2]) ]
#    trainingData = sc.parallelize(trainData)
#    print trainingData.collect()
#    return trainingData

    RDD_Subjects = sc.textFile('sparkrun_tf.csv')
    RDD_Subjects.cache()
    Subject = ",{0},".format(Subject)

    tfs = open('sparkrun_tf.csv')

    # Data list to return    
    myData = []
    
    for tfline in tfs:
        # Get the info for the tf
        tf_info = tfline.split(',')
        tf_file = tf_info[0]
        tf_fold = tf_info[1].strip()
        tf_id = tf_info[2]
        
        # if it's not in the folds we want don't bother
        if tf_fold not in Folds:
            continue
        
        # Find what subjects it has
        SubjectLabel = 0.0        
        # Get the line for this file whch has all its subjects
        RDD_test = RDD_Subjects.filter(lambda line: tf_id in line)
        if report_diagnostics:
            print ""
            print "looking for ",tf_id
            print "found ",RDD_test.collect()
            
        # test to see if the subject we want applies to this file
        RDD_test = RDD_test.filter(lambda line: Subject in line)
        if RDD_test.count() > 0:
            SubjectLabel = 1.0
            
        if report_diagnostics:
            print "looking for ",Subject," status ",SubjectLabel

        # Get the pickled TF vector and add to the list                    
        RDD_TF = sc.pickleFile('tfs/' + tf_file)
        # Make a hash vector
        RDD_HASH = return_HashVector(sc, RDD_TF, HashSize)
        myData.append((SubjectLabel, RDD_HASH.collect()))
                
#     Finished getting all the files
    RDD_Data = sc.parallelize(myData)
    return RDD_Data
# =====================================================================
# Prepare data for Naive Bayes model from tfidf and subject
# This function returns an RDD of the hash vectors from the folds 
# specified with a label of 1.0 if the file contains the subject
# and 0.0 otherwise
# =====================================================================
def return_NB_data(sc, Subject, Folds, diag_file, runtimes_file, report_diagnostics):
    report_diagnostics = False
    # Very simple toy data so I know what I'm aiming for
#    trainData = [(0.0, [0.0, 3.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (1.0, [2.0, 1.2]) ]
#    trainingData = sc.parallelize(trainData)
#    print trainingData.collect()
#    return trainingData

    RDD_Subjects = sc.textFile('sparkrun_idf.csv')
    RDD_Subjects.cache()

    Subject = ",{0},".format(Subject)

    # Data list to return    
    myData = []
    
    for fold in Folds:
        data_path = str(fold) + '/'
        for files in os.walk(data_path):
            for each_file in files:
                if not '.tfidf' in each_file:
                    continue
                
                # got a .tfidf, get the file name and look up what subjects it has
                search = each_file[2:-3]
                SubjectLabel = 0.0
                
                # Get the line for this file whch has all it's subjects
                RDD_test = RDD_Subjects.filter(lambda line: search in line)
                if report_diagnostics:
                    print ""
                    print "looking for ",search
                    print "found ",RDD_test.collect()
                    
                # test to see if the subject we want applies to this file
                RDD_test = RDD_test.filter(lambda line: Subject in line)
                if RDD_test.count() > 0:
                    SubjectLabel = 1.0
                    
                if report_diagnostics:
                    print "looking for ",Subject," status ",SubjectLabel

                # Get the pickled TFIDF vector and add to the list                    
                RDD_TFIDF = sc.pickleFile(each_file)
                myData.append((SubjectLabel, RDD_TFIDF.collect()))
                
    # Finished getting all the files
    RDD_Data = sc.parallelize(myData)
    return RDD_Data
# ===============================================
# test what to do with RDDs of hashed values
#    f1 = sc.parallelize([0.0, 3.4])
#    f2 = sc.parallelize([1.0, 4.3])
#    f3 = sc.parallelize([2.0, 1.2])
#    
#    trainData = []
#    trainData.append((0.0, f1.collect()))
#    trainData.append((1.0, f2.collect()))    
#    trainData.append((1.0, f3.collect()))    
#    print "TRAIN ",trainData

#    trainingData = sc.parallelize(trainData)
#    #print trainingData.collect()
#    return trainingData
        
# =====================================================================
# DECISION TREES    
# =====================================================================
# print the different performance metrics
def accuracy(rm):
    resultMap = defaultdict(lambda :0,rm) # use of defaultdic saves checking for missing values
    total = sum(resultMap.values())
    truePos = resultMap[(1,1,)]
    trueNeg = resultMap[(0,0,)]
    return ( float(truePos+trueNeg)/total )

def model_run_DecisionTrees(sc, maxDepth, trainingData, testingData, validateData):

    # ======================================================================
    # Training Data 
    startTime = time()
    fileNum = trainingData.count()
    
    print ""    
    print "Running with maxdepth {0}, for {1} sample files".format(maxDepth, fileNum)

    trainingLP = trainingData.map(lambda (x,l): LabeledPoint(x,l))
    dtModel = DecisionTree.trainClassifier(trainingLP, numClasses=2, \
                                         categoricalFeaturesInfo={}, \
                                         impurity="entropy", \
                                         maxDepth=maxDepth, \
                                         maxBins=5)
                                         
    predictedLabels = dtModel.predict(trainingLP.map(lambda lp : lp.features))
    trueLabels = trainingLP.map(lambda lp : lp.label)
    resultsTrain = trueLabels.zip(predictedLabels)
    resultMap = resultsTrain.countByValue()
    trainAccuracy = accuracy(resultMap)
    
    # ======================================================================
    # Test the Data 
    testingLP = testingData.map(lambda (x,l): LabeledPoint(x,l))
    predictions = dtModel.predict(testingLP.map(lambda x: x.features))
    resultsTest = testingLP.map(lambda lp: lp.label).zip(predictions)
    resultMapTest = resultsTest.countByValue()
    testAccuracy = accuracy(resultMapTest)

    # ======================================================================
    # Validate the model 
    validateLP = validateData.map(lambda (x,l): LabeledPoint(x,l))
    predictions = dtModel.predict(validateLP.map(lambda x: x.features))
    resultsTest = validateLP.map(lambda lp: lp.label).zip(predictions)
    resultMapVal = resultsTest.countByValue()
    valAccuracy = accuracy(resultMapVal)

    # ======================================================================
    # Report Stats
    elapsedTime = time() - startTime
    return [['time taken : ' , elapsedTime],\
            ['training accuracy : ' , trainAccuracy],
            ['validation accuracy : ' , valAccuracy],
            ['testing accuracy : ' , testAccuracy]]

def model_DecisionTrees(sc, HashSize, diag_file, runtimes_file, report_diagnostics):
#    # Very simple toy test datasets so I know what I'm aiming for
#    trainData = [(0.0, [0.0, 3.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (1.0, [2.0, 1.2]) ]
#
#    testData = [(0.0, [0.0, 3.4]), \
#                (0.0, [3.0, 1.3]), \
#                (1.0, [2.5, 1.2]) ]
#
#    validData = [(0.0, [2.0, 5.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (0.0, [2.5, 1.2]) ]

    # Get the top 10 subjects
    Subject_List = get_top10_Subjects(sc, 'tf', diag_file, runtimes_file, report_diagnostics)

    # Specify the FOLDs containing TF.IDF hash vectors for the testing and training set
    # Doing it this way makes it easy to adjust the proportion of testing/training/validation sample

#    Try it with both tfs and tf.IDFs to see what difference it makes. Don't really have
#    enough tf.idfs as I only produced 50 before the memory blew
#    trainFolds = ['0','1','2','3']
#    testFolds = ['4','5','6']
#    validFolds = ['7','8','9']
   
    trainFolds = [0,1,2,3,4,5,6]
    testFolds = [7,8]
    validFolds = [9]

    # write a csv file of the results for various maxDepth values
    rpt = open('4c_DT_Stats.csv','a')
    print >> rpt,"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}".format('Subject', \
                                                      'HashSize', \
                                                      'maxDepth', \
                                                      'Train File count', \
                                                      'Test File count', \
                                                      'Validate File count', \
                                                      'Time', \
                                                      'Train accuracy', \
                                                      'Validation accuracy', \
                                                      'Testing accuracy')
    for maxDepth in [2,3,4] :
        for Subject in Subject_List:
            
            Subject = re.sub('\n','',Subject[1])
            Subject = Subject.strip()

            print ""
            print "DECISION TREES FOR Subject = {0}, maxDepth {1}, HashSize {2}".format(Subject, maxDepth, HashSize)
         
            # Make data RDDs which will be converted to LabeledPoints later in the model run
            # Use this when running tfs
#            trainingData = return_data(sc, Subject, HashSize, trainFolds, diag_file, runtimes_file, report_diagnostics)
#            testingData = return_data(sc, Subject, HashSize, testFolds, diag_file, runtimes_file, report_diagnostics)    
#            validateData= return_data(sc, Subject, HashSize, validFolds, diag_file, runtimes_file, report_diagnostics)     
    
            # Make data RDDs which will be converted to LabeledPoints later in the model run
            # Use this when running tf.idfs
            trainingData = return_NB_data(sc, Subject, trainFolds, diag_file, runtimes_file, report_diagnostics)
            testingData = return_NB_data(sc, Subject, testFolds, diag_file, runtimes_file, report_diagnostics)    
            validateData= return_NB_data(sc, Subject, validFolds, diag_file, runtimes_file, report_diagnostics)     

            dtResult = model_run_DecisionTrees(sc, maxDepth, trainingData, testingData, validateData)
            for metric in dtResult:        
                print metric[0], metric[1]
            print >> rpt,"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}".format(Subject, HashSize, \
                                                              maxDepth, \
                                                              trainingData.count(), \
                                                              testingData.count(), \
                                                              validateData.count(), \
                                                              dtResult[0][1], \
                                                              dtResult[1][1],
                                                              dtResult[2][1],
                                                              dtResult[3][1])
            
    rpt.close()
    print ""
    print "DECISION TREES FINISHED."
    
# =====================================================================
# NAIVE BAYES    
# =====================================================================
# print the different performance metrics
def printMetrics(RunType,HashSize, Subject, rm, fileNum, RunTime, UsedLog):
    
    resultMap = defaultdict(lambda :0,rm)
    truePos = resultMap[(1,1,)]
    falsePos = resultMap[(0,1,)]
    trueNeg = resultMap[(0,0,)]
    falseNeg = resultMap[(1,0,)]
    
    print 'total files in run = ', fileNum
    print 'positive ground truth = ', truePos + falseNeg
    print 'negative ground truth = ', falsePos + trueNeg
    print 'truePos = ', truePos
    print 'falsePos = ', falsePos
    print 'trueNeg = ', trueNeg
    print 'falseNeg = ', falseNeg
    print 'errRate = ', float(falseNeg+falsePos)/fileNum
    Accuracy = float(truePos+trueNeg)/fileNum
    print 'accuracy = ', Accuracy
    try:
        print 'recall  = ', float(truePos)/(truePos+falseNeg)
    except:
        print 'recall  = div by 0'

    try:
        print 'precision = ', float(truePos)/(truePos+falsePos)
    except:
        print 'precision  = div by 0'

    try:
        print 'specificity = ', float(trueNeg)/(trueNeg+falsePos)
    except:
        print 'specificity  = div by 0'

    rpt = open('4d_NB_Stats.csv','a')
    print >> rpt,"{0},{1},{2},{3},{4},{5},{6}".format(RunType,Subject,fileNum, HashSize, RunTime, Accuracy, UsedLog)
    rpt.close()

def model_run_NaiveBayes(sc, HashSize, Subject, trainingData, testingData):
    
    print "TRAINING NAIVE BAYES"
    start_time = time()
    fileNum = trainingData.count()
    # create the LabeledPoint
    trainingLP = trainingData.map(lambda (x,l): LabeledPoint(x,l))
    # Train the model
    nbModel = NaiveBayes.train(trainingLP, 1.0)    
    resultsTrain = trainingData.map(lambda (l,v) :  ((l, nbModel.predict(v)),1))
    resultsTrain = resultsTrain.reduceByKey(add)
    resultMap = resultsTrain.collectAsMap()
    printMetrics("Training",HashSize, Subject, resultMap, fileNum, time()-start_time,'True')
                
    print ""
    print 'TEST RESULTS'
    start_time = time()
    fileNum = testingData.count()
    resultsTest = testingData.map(lambda (l,v):  ((l,nbModel.predict(v)),1)).reduceByKey(add)
    resultMapTest = resultsTest.collectAsMap()
    printMetrics("Testing",HashSize,Subject, resultMapTest, fileNum, time()-start_time,'True')
                    
def model_NaiveBayes(sc, diag_file, runtimes_file, report_diagnostics):
    
    # Get the top 10 subjects
    Subject_List = get_top10_Subjects(sc, 'idf', diag_file, runtimes_file, report_diagnostics)

    # Specify the FOLDs containing TF.IDF hash vectors for the testing and training set
    # Doing it this way makes it easy to adjust the proportion of testing/training sample
    trainFolds = [0,1,2,3,4,5,6]
    testFolds = [7,8,9]
    
    HashSize = 10000
    for Subject in Subject_List:
        
        Subject = Subject[1]
        print ""
        print "NAIVE BAYES FOR Subject = {0}".format(Subject)
        # Make data RDDs which will be converted to LabeledPoints later in the model run
        trainingData = return_NB_data(sc, Subject, trainFolds, diag_file, runtimes_file, report_diagnostics)
        testingData = return_NB_data(sc, Subject, testFolds, diag_file, runtimes_file, report_diagnostics)    
        model_run_NaiveBayes(sc, HashSize, Subject, trainingData, testingData)

    print ""
    print "NAIVE BAYES FINISHED."

# =====================================================================
# LOGISTIC REGRESSION    
# =====================================================================

def model_LogisticRegression(sc, HashSize, diag_file, runtimes_file, report_diagnostics):
#    # Very simple toy test datasets so I know what I'm aiming for
#    trainData = [(0.0, [0.0, 3.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (1.0, [2.0, 1.2]) ]
#
#    testData = [(0.0, [0.0, 3.4]), \
#                (0.0, [3.0, 1.3]), \
#                (1.0, [2.5, 1.2]) ]
#
#    validData = [(0.0, [2.0, 5.4]), \
#                 (1.0, [1.0, 4.3]), \
#                 (0.0, [2.5, 1.2]) ]

    Subject_List = get_top10_Subjects(sc, 'tf', diag_file, runtimes_file, report_diagnostics)

    # Specify the FOLDs containing TF.IDF hash vectors for the testing and training set
    # Doing it this way makes it easy to adjust the proportion of testing/training/validation sample
    # try with tfs
#    trainFolds = ['0','1','2','3']
#    testFolds = ['4','5','6']
#    validFolds = ['7','8','9']

    trainFolds = [0,1,2,3,4,5,6]
    testFolds = [7,8]
    validFolds = [9]

    # write a csv file of the results for various maxDepth values
    rpt = open('4c_LR_Stats.csv','a')
    print >> rpt,"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}".format('Subject', \
                                                      'HashSize', \
                                                      'maxDepth', \
                                                      'Train File count', \
                                                      'Test File count', \
                                                      'Validate File count', \
                                                      'Time', \
                                                      'Train accuracy', \
                                                      'Validation accuracy', \
                                                      'Testing accuracy')
    for maxDepth in [2,3,4] :
        for Subject in Subject_List:
            
            Subject = re.sub('\n','',Subject[1])
            Subject = Subject.strip()

            print ""
            print "LOGISTIC REGRESSION FOR Subject = {0}, maxDepth {1}, HashSize {2}".format(Subject, maxDepth, HashSize)
         
            # Make data RDDs which will be converted to LabeledPoints later in the model run
#            use with .tfs
#            trainingData = return_data(sc, Subject, HashSize, trainFolds, diag_file, runtimes_file, report_diagnostics)
#            testingData = return_data(sc, Subject, HashSize, testFolds, diag_file, runtimes_file, report_diagnostics)    
#            validateData= return_data(sc, Subject, HashSize, validFolds, diag_file, runtimes_file, report_diagnostics)     

            # Use this when running tf.idfs
            trainingData = return_NB_data(sc, Subject, trainFolds, diag_file, runtimes_file, report_diagnostics)
            testingData = return_NB_data(sc, Subject, testFolds, diag_file, runtimes_file, report_diagnostics)    
            validateData= return_NB_data(sc, Subject, validFolds, diag_file, runtimes_file, report_diagnostics)     

            lrResult = model_run_LogisticRegression(sc, trainingData, testingData, validateData)
            for metric in lrResult:        
                print metric[0], metric[1]
            print >> rpt,"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}".format(Subject, HashSize, \
                                                              maxDepth, \
                                                              trainingData.count(), \
                                                              testingData.count(), \
                                                              validateData.count(), \
                                                              lrResult[0][1], \
                                                              lrResult[1][1],
                                                              lrResult[2][1],
                                                              lrResult[3][1])
                
        print ""
        print "LOGISTIC REGRESSION FINISHED."

def model_run_LogisticRegression(sc, trainingData, testingData, validateData):
    #train_lbl_wordCountL, test_lbl_wcl, val_lbl_wcl
    startTime = time()
    regParam = 0.3

    # TRAINING
    trainingLP = trainingData.map(lambda (x,l): LabeledPoint(x,l))    
    model = LogisticRegressionWithSGD.train(trainingLP,miniBatchFraction=0.1,regType='l1', intercept=True, regParam=regParam)
    resultsTrain = trainingLP.map(lambda lp :  (lp.label, model.predict(lp.features)))
    resultMap = resultsTrain.countByValue()
    trainAccuracy = accuracy(resultMap)

    # TESTING
    testingLP = testingData.map(lambda (x,l): LabeledPoint(x,l))
    resultsTest = testingLP.map(lambda lp :  (lp.label, model.predict(lp.features)))
    resultMapTest = resultsTest.countByValue()
    testAccuracy = accuracy(resultMapTest)
    
    # VALIDATE
    validateLP = validateData.map(lambda (x,l): LabeledPoint(x,l))
    predictions = validateLP.map(lambda x: model.predict(x.features))
    resultsTest = validateLP.map(lambda lp: lp.label).zip(predictions)
    resultMapVal = resultsTest.countByValue()
    valAccuracy = accuracy(resultMapVal)
    
    elapsedTime = time() - startTime
    return [['time taken : ' , elapsedTime],\
            ['training accuracy : ' , trainAccuracy],
            ['validation accuracy : ' , valAccuracy],
            ['testing accuracy : ' , testAccuracy]]

