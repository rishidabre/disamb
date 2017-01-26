#!/usr/bin/python

# This script identifies the clusters containing papers belonging to more than one author IDs

from py2neo import authenticate, Graph, watch, error#, DBMS
from py2neo.packages.httpstream import http
http.socket_timeout = 9999
import getpass
import time
import mmap

#import logging
#logging.basicConfig(format='%(asctime)s %(message)s', filename='test.log', level=logging.DEBUG)
#logging.basicConfig(level=logging.DEBUG)

def currenttime():
    return round(time.time()*1000)

passwd=getpass.getpass()

# set up authentication parameters
authenticate("neo4j-genbank.syr.edu:7474", "neo4j", passwd)

watch("httpstream")
# connect to authenticated graph database
sgraph = Graph("http://neo4j-genbank.syr.edu:7474/db/data/")
sgraph.cypher

path_paperauthoraffil="temp/temp_paperauthoraffil2.txt"

t1=currenttime()
with open(path_paperauthoraffil, 'r') as f_paperauthoraffil:
    mmap_paperauthoraffil=mmap.mmap(f_paperauthoraffil.fileno(), 0, access=mmap.ACCESS_READ)
    clusters_l2=sgraph.cypher.execute("MATCH (c2:ClusterL2) RETURN ID(c2) AS CLUSTER2_ID")
    for cluster_l2_record in enumerate(clusters_l2.records):
        main_author_id=-1
        cluster_l2_id=cluster_l2_record.__getitem__(1).__getitem__(0)
        get_clusters_query="MATCH (c2:ClusterL2)<-[r:BELONGS_TO]-(c1:ClusterL1) WHERE ID(c2)="+str(cluster_l2_id)+" RETURN ID(c1) AS CLUSTER_ID"
        print(get_clusters_query)
        clusters=sgraph.cypher.execute(get_clusters_query)
        for cluster_record in enumerate(clusters.records):
            author_id=-1
            cluster_id=cluster_record.__getitem__(1).__getitem__(0)
#            print("For Cluster ID: "+str(cluster_id))
            get_papers_query="MATCH (c:ClusterL1)<-[r:BELONGS_TO]-(p:PaperID) WHERE ID(c)="+str(cluster_id)+" RETURN p.pid AS PAPER_ID"
            print(get_papers_query)
            papers=sgraph.cypher.execute(get_papers_query)
            print("Papers: "),
            print(papers)
            for paper_record in enumerate(papers.records):
                paper_id=paper_record.__getitem__(1).__getitem__(0)
                index=mmap_paperauthoraffil.find(paper_id)
                current_line=""
                if index!=-1:
                    mmap_paperauthoraffil.seek(index)
                    current_line=mmap_paperauthoraffil.readline().replace('\n','').replace('\r','')
                    current_author_id=current_line.split('\t')[1]
#                    print("Current Author ID: "+str(current_author_id))
                    mmap_paperauthoraffil.seek(0)
                if author_id==-1:
                    author_id=current_author_id
                    main_author_id=author_id
                if author_id!=current_author_id:
                    print("Author ID mismatch in Cluster ID: "+str(cluster_id)),
                    print(" Concerned row: "+current_line)
                if main_author_id==-1:
                    main_author_id=author_id
#            print("Main Author ID: "+str(main_author_id))
#            if main_author_id!=author_id:
#                print("Mismatch!")
t2=currenttime()
print("Total time taken: "+str(round(t2-t1))+" millis")
