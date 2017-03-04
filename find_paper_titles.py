#!/usr/bin/python

# This script identifies the clusters containing papers belonging to more than one author IDs

from py2neo import authenticate, Graph, watch, error#, DBMS
from py2neo.packages.httpstream import http
http.socket_timeout = 9999
import getpass
import time
import mmap
import codecs

def currenttime():
    return round(time.time()*1000)

passwd=getpass.getpass()

# set up authentication parameters
authenticate("neo4j-genbank.syr.edu:7474", "neo4j", passwd)

watch("httpstream")
# connect to authenticated graph database
sgraph = Graph("http://neo4j-genbank.syr.edu:7474/db/data/")
sgraph.cypher

path_paperauthoraffil="temp/temp_paperauthoraffil5.txt"
path_papertitles="paper_titles_manning_all.txt"

papers=[]

t1=currenttime()
with open(path_paperauthoraffil, 'r') as f_paperauthoraffil:
    for line in f_paperauthoraffil:
        line_split=line.replace('\n','').replace('\r','').split('\t')
        paper_id=line_split[0]
        papers.append(paper_id)

result=sgraph.cypher.execute("match (c1:ClusterL1)<-[]-(p:PaperID) where p.pid in "+str(papers)+" return id(c1) as ClusterL1ID, p.pid as PaperID")
paper_cluster={}
for record in result:
    paper_id=record.__getitem__(1).encode('utf-8')
    cluster_l1_id=record.__getitem__(0)
    cluster=paper_cluster.get(paper_id)
    paper_cluster[paper_id]=cluster_l1_id
print(paper_cluster)

query_to_get_all_l2_clusters="match (c2:ClusterL2)<-[r]-(c1) return id(c2),id(c1)"
result=sgraph.cypher.execute(query_to_get_all_l2_clusters)
cluster_l1_l2={}
for record in result:
    cluster_l2_id=record.__getitem__(0)
    cluster_l1_id=record.__getitem__(1)
    cluster_l1_l2[cluster_l1_id]=cluster_l2_id
print(cluster_l1_l2)

with codecs.open(path_papertitles,'w','utf-8') as f_papertitles:
    result=sgraph.cypher.execute("match (p:Paper) where p.PaperID in "+str(papers)+" return p.PaperID as ID, p.Title as Title")
    for record in result:
        paper_id=record.__getitem__(0).encode('utf-8')
        paper_title=record.__getitem__(1).encode('utf-8')
        cluster_id=paper_cluster.get(paper_id)
        if not cluster_id:
            cluster_id=-1
        else:
            cluster2_id=cluster_l1_l2.get(cluster_id)
            if cluster2_id:
                cluster_id="C2_"+str(cluster2_id)
            else:
                cluster_id="C1_"+str(cluster_id)
        try:
            f_papertitles.write(paper_id+"\t"+str(cluster_id)+"\t"+paper_title+"\n")
        except:
            f_papertitles.write(paper_id+"\t"+str(cluster_id)+"\t"+paper_title.decode('utf-8')+"\n")
            print(">>>"),
        print(paper_id+"\t"+str(cluster_id)+"\t"+paper_title)
t2=currenttime()
print("Total time taken: "+str(round(t2-t1))+" millis")
