import re
# -------------------------------------------------------------
# Find the first line of the header information
# NOTE : *** START isn't enough because there is a also a line
#        *** START: FULL LICENCE which is the start of the licence information
# -------------------------------------------------------------
def Header(RDD_file, diag_file, file_name, report_diagnostics):
    Header_Types = ['^\*\*\* START (.*)\/*/*/*$',\
					'^\*\*\*The Project Gutenberg(.*)',\
                    '^\*\*\*START OF THE PROJECT GUTENBERG EBOOK(.*)',\
                    '^\*END THE SMALL PRINT',\
					'\*END*THE SMALL']

    got_Header = False
    myHeader = ''
	
    for Header in Header_Types:
        print >> diag_file, "trying ",Header
    	hasHeader = RDD_file.filter(lambda line: re.search(Header, line))
        if hasHeader.count() > 0:
            myHeader = Header
            got_Header = True
            break

    if got_Header == True:
        if report_diagnostics == True:
            out = hasHeader.collect()
            print >> diag_file, "Header is ",out

    if got_Header == False:
	    print >> diag_file,"FATAL ERROR, no Header in file ", file_name

    return myHeader
