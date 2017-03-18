import re
# -----------------------------------------------------------------------
# Test for things that might (or might not) happen at the end of the file
# -----------------------------------------------------------------------
def Footer(RDD_file, diag_file, file_name, report_diagnostics):
    Footer_Types = ['^\*\*\* END OF THIS (.*)\/*/*/*$',\
		    '^\*\*\* START: FULL LICEN(.*)']
    got_Footer = False
    myFooter = ''
	
    for Footer in Footer_Types:
    	hasFooter = RDD_file.filter(lambda line: re.search(Footer, line))
        if hasFooter.count() > 0:
            myFooter = Footer
            got_Footer = True
            break

    if got_Footer == True:
        if report_diagnostics == False:
            out = hasFooter.collect()
            print >> diag_file," Footer line ",out

    return myFooter

