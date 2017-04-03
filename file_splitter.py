#!/usr/bin/python

from sys import argv
from os import path, makedirs
from time import time
import subprocess

# Splits the given file into chunks
def split_file(fpath, keylen):
    t1=time()
    dname=fpath[fpath.rfind('/')+1:-4]
    # Check if directory to store chunks exists and create if not
    if not path.exists(dname):
        makedirs(dname)
    # Create a dict of handles for each file
    fhandles={}
    i=0 # Counter
    with open(fpath, 'r') as f:
        # Get the total number of lines for accounting
        total_lines=subprocess.check_output(["grep", "-vc", "'^$'", fpath]).replace('\n','')
        print "Total lines: "+total_lines
        for line in f:
            i+=1
            # Make key from the first column entry, precisely the sequence of first 3 characters
            key=line.split('\t')[0][:keylen]
            # Check if a handle to file with given prefix exists
            fhandle=fhandles.get(key)
            if fhandle==None:
                # No handle to file with given key exists, create one
                fhandle=open(dname+'/'+key+'.txt','a')
                fhandles[key]=fhandle
            # Write the line to file
            fhandle.write(line)
            print "[%s of %s]"%(i,total_lines)
    # Close all the file handles
    for fhandle in fhandles.values():
        fhandle.close()
    t2=time()
    print "Total files created: %s"%len(fhandles)
    print "Total time taken to split %s: %s second(s)"%(fpath,t2-t1)
    return True

if __name__=='__main__':
    if len(argv)!=3:
        print "Splits the MAG data file."
        print "Usage: %s <file_path> <key_length>"%(argv[0])
        quit()
    fpath=argv[1]
    keylen=argv[2]
    print "Given path: %s"%fpath
    print "Key Length: %s"%keylen
    split_file(fpath,int(keylen))
