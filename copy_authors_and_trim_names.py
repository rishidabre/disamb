#!/usr/bin/python

import codecs
import time

#path_authors="/scrp/Authors.txt"
#path_authors_trimmed="/scrp/AuthorsTrimmed.txt"

path_authors="/disamb/temp/temp_authors2.txt"
path_authors_trimmed="/disamb/temp/temp_authors2_trimmed.txt"

t1=time.time()
with codecs.open(path_authors,"r","utf-8") as f_authors:
    with codecs.open(path_authors_trimmed,"w+","utf-8") as f_authors_trimmed:
        for line in f_authors:
            line_split=line.replace('\n','').replace('\r','').split('\t')
            author_name=line_split[1]
            author_name_split=author_name.split()
            author_name_trimmed=author_name_split[0][0]+" "+author_name_split[len(author_name_split)-1]
            new_line=line.replace(line_split[1],author_name_trimmed)
            f_authors_trimmed.write(new_line)
t2=time.time()
print("Time taken: "+str(t2-t1)+" second(s)")
