# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 06:48:50 2014

@author: macuser
"""

# ===========================================================================
# Pre-cursors to running this file
#
# After some initial trials, I decided to do this in stages rather than in one run. 
#
# Stage 1.
# I run CW-Part2.py first of all. This analyses the meta data, and creates a 
# file called XML.csv as one of its outputs. XML.csv is of the form -
#
#   ID=File_Id, subject, subject, subject ...
#
# USAGE -> spark-submit CW-Part2.py
#
# 25/Nov/2014
# Justification : I thought I would do Part 2 as the first step, as doing it second doesn't 
# allow for the condition that there might be .txt files
# not represented in the meta data in which case they must be excluded from the analysis.
#
# Stage 2.
# A py file called preprocess-01.py iterates through all the .txt files in the
# data directory. It identifies .txt files with Duplicate Ids, missing Ids, files with no 
# meta data (from XML.csv). It creates a file called file_analysis.lst which 
# has a record for each .txt in the corpus tagged by GOOD, DUPLICATE etc. I can analyse
# file_analysis.lst (quickly in Spark) to give me file status counts. I use file_analysis.lst
# in later runs so I can identify .txt files which are good to use, and I have the document Id
# for each .txt pre-computed, saving a little bit of running time
#
# USAGE -> spark-submit preprocess-01.py EXCLUDES file_to_create
#
# Stage 3.
# preprocess-01.py can also analyse a x.lst file of files created with ls -lhR /data/extra ... > x.lst
# if looks for files with filesizes greater than a user-specified value and returns a python-format 
# include file which will exclude those files from the main run. This for this like the Human genome
# I did this as a separate stage so I could alter the parameter here.
#
# USAGE -> spark-submit preprocess-01.py BIG listing_file
#
# Stage 4.
# Get counts of file types 
#
# USAGE -> spark-submit preprocess-01.py ANALYSIS input_file (from Stage 2 or Stage 3)
#
# ===========================================================================

# ===========================================================================
# This file
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
 
