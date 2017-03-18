# -*- coding: utf-8 -*-
"""
Created on Sat Nov  8 12:54:30 2014

@author: Suzanne
"""
import xml.etree.ElementTree as ET
from subprocess import call
import unicodedata

from time import time
import socket
import codecs
import re
import os

from pyspark import SparkConf, SparkContext

# ========================================================================
# PURPOSE OF THIS FILE
# This file analyses the Project Gutenberg XML meta data, creating
# a csv file of BookId, Subjects, and also an RDD to satisfy the coursework
# specification, but I think the CSV is easier to use with the RDD.filter
# construct at later tages in the process.
# It looks for Subject codes in the Subject/Description/Value element
# and stores all subjects with the file Id
# Then the top 10 most frequent subjects are determined
# the instructions say to save an RDD of the FileId, Subject data
# but I didn't see the point, I can just read in my XML.csv and use that
# filtering on BookId
# ========================================================================

# CREATES : XML.csv, MetaData.RDD

# =====================================================================
# Global variables
# =====================================================================
stop_after = 0          # Stop after processing this number of files. If 0 then do everything
batch_size = 50         # Report progress after this many files have been processed
output_file = 'XML.csv' # output to this file
# =====================================================================
# Find the subjects for a file
# If I were doing this "properly" I'd parse the namespaces, but I did this
# before lxml was working on lewes and this method seemed easy and it 
# worked Ok so I went with it. It relies on the structure of finding
# tags subsequent to each other that are subject/Description/value
# =====================================================================
def find_xml_subjects(file_name):

    tree = ET.parse(file_name)
    root = tree.getroot()

    Id = "Missing"      
    Subjects = []
	
    current_tag = ""

    # element Object is structured -
    # child.tag = string
    # child.attrib = dictionary of {attribute_name : value} as there may be > 1 attributes
    # child.text = text of element    

    for child in root.iter():
        # process the "ebook" element
        if re.match('{.*}ebook',child.tag):
            # Get "about" attribute from "ebook" element
            for key, value in child.attrib.items():   # iterate the attribute dictionary
                if 'about' in key:
                    Id = value

        # Process the "subject element"
	if re.match('{.*}subject',child.tag):
	    current_tag = "subject"
        
	if current_tag == "subject" and 'Description' in child.tag:
	    current_tag = "subject/Description"
            
	if current_tag == "subject/Description" and "value" in child.tag:   
            Subjects.append(child.text)
            current_tag = ""

    # End of file
    #Remove the ebook/ text from the Id
    Id = re.sub('ebooks/','', Id)

    SubjectList = ""
    for sub in Subjects:
        if len(SubjectList) > 0:
            SubjectList += ','

        SubjectList += sub
       
    # I'll use ID=fileID to search for later on 
    return "ID=" + Id + ',' + SubjectList

# =====================================================================
# Iterate over all the files
# =====================================================================
def walk_through_all_xml_files(data_path_xml):

	start_time = time()
        batch_time = time()
        this_time = time()

	print ("Function walk_through_all_xml_files, Starting at ", start_time)
    # output to a file
        NonUTF8 = 0
	out = open(output_file,'w')

        print ("Starting meta data analysis at ",start_time)
        print ("")

        file_counter = 0
        for root, dirs, files in os.walk(data_path_xml, topdown=False):
            for name in files:
                file_counter +=1
                if stop_after > 0 and file_counter > stop_after:
                    break

                # Let me know how it's going every 'batch_size' files
                if file_counter % batch_size == 0:
                    print (file_counter," files processed, time taken {0:.2f}".format(time() - start_time), " last batch took {0:.2f}".format(time() - batch_time))
                    batch_time = time()

                CSV = find_xml_subjects(root + "/" + name)
                try:
                    print >> out, CSV
                except:
                    NonUTF8 +=1
                    continue

        out.close()
        print "function walk_through_all_xml_files, Finished at ", time()
        print "found {0} non utf8 files".format(NonUTF8)
  
def xx(line):
    x = line.split(',')
# ========================================================================================
# Finally, run the program
# ========================================================================================

if __name__ == "__main__":
    MyComputer=socket.gethostname()
    
    # I can run this from home or Uni without having to edit
    # or type unneccesary arguments which I keep mis-spelling
    # or forgetting that linux is case sensitive
    if MyComputer[0:4]=='Suza':
        data_path = "C:/Users/Public/Documents/MSc-DataScience/INM432/CW/"
    else:
        data_path = "/data/extra/gutenberg/meta/"

    # Create a .csv file of all the subjects
    print ""
    print "CREATE CSV OF META DATA"
    walk_through_all_xml_files(data_path) 

    print ""
    print "FINISHED, GETTING TOP 10 SUBJECTS"
	# Read the file as a spark RDD
    conf = (SparkConf().setMaster("local[2]").setAppName("XML Analysis"))
    sc = SparkContext(conf = conf)

    RDD_XML = sc.textFile('XML.csv')
    print "Count of files : ",RDD_XML.count()

    # Split into terms
    RDD_FileSubjects = RDD_XML.flatMap(lambda line:line.split(','))

    RDD_FileSubjects = RDD_FileSubjects.filter(lambda term: len(term) > 0)
    output = RDD_FileSubjects.collect()
    print "Count of terms (inc filename) : ",RDD_FileSubjects.count()

    # Split into terms, 1
    RDD_FileSubjects = RDD_FileSubjects.flatMap(lambda term:[((term),1)])
    # reduce
    RDD_FileSubjects = RDD_FileSubjects.reduceByKey(lambda term, count: term + count)
    # swap Key, value so we can order by value
    RDD_FileSubjects = RDD_FileSubjects.map(lambda (x,y): (y,x)).sortByKey(ascending=False)

    output = RDD_FileSubjects.collect()
    print "CONTENT"
    print "count ",RDD_FileSubjects.count()
    i = 0
    for Subject in output:
       i +=1
       if i > 10:
           break
       print Subject

    # Save RDD to disk.
    save_name = 'MetaData.RDD'
    call(["rm", "-rf", save_name])
    try:
        RDD_FileSubjects.saveAsPickleFile(save_name)
    except:
        print "problem saving file"
    sc.stop()








