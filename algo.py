#!/usr/bin/python

import logging
import getpass
from py2neo import authenticate, Graph, watch
import time
import codecs
import mmap
import sys
import subprocess
import notifier

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

# Email notification settings
notification_sender="rrdabre@syr.edu"
notification_receivers=["rrdabre@syr.edu"]
notification_subject="Disambiguation Algorithm Notification"

# Set the file paths
path_authors="/scrp/AuthorsTrimmed.txt"
path_paperauthoraffil="/mnt/MicrosoftAcademicGraph/PaperAuthorAffiliations/PaperAuthorAffiliations.txt"
path_paperreferences="/mnt/MicrosoftAcademicGraph/PaperReferences/PaperReferences.txt"

#path_authors='/disamb/temp/temp_authors6_trimmed.txt'
#path_paperauthoraffil='/disamb/temp/temp_paperauthoraffil6.txt'
#path_paperreferences='/disamb/temp/temp_paperreferences6.txt'

path_prevpos="/disamb/prevpos.txt"

# Set the alpha and beta values
alpha_a=0.54
alpha_s=0.75
alpha_r=0.19
alpha_c=1.02
beta_1=1
beta_2=0.19
beta_3=0.011
beta_4=0.49

# Credentials of Neo4j repository
conn_protocol="http"
surl="neo4j-genbank.syr.edu:7474"
uname="neo4j"
passwd=getpass.getpass()

# Authenticate with the repository
authenticate(surl, uname, passwd)

# Connect to the graph database
neo_graph=Graph(conn_protocol+"://"+surl+"/db/data/")

# Check if the authentication is working by acquiring the cypher resource
cypher_resource=neo_graph.cypher

# Length of the keys with which the file chunks are named
_KEYLEN_=3

# Paths to the directories where file chunks are stored (absolute or relative to current directory)
dir_paperauthoraffil="PaperAuthorAffiliations"
dir_paperreferences="PaperReferences"

# Returns the paths to the PaperAuthorAffiliations chunk files containing given paper IDs
def get_paperauthoraffil_chunk_file_paths(paper1_id, paper2_id):
    key1=paper1_id[0:_KEYLEN_]
    fpath1=dir_paperauthoraffil+"/"+key1+".txt"
    key2=paper2_id[0:_KEYLEN_]
    fpath2=dir_paperauthoraffil+"/"+key2+".txt"
    return [fpath1, fpath2]

# Returns the paths to the PaperReferences chunk files containing given paper IDs
def get_paperreferences_chunk_file_paths(paper1_id, paper2_id):
    key1=paper1_id[0:_KEYLEN_]
    fpath1=dir_paperreferences+"/"+key1+".txt"
    key2=paper2_id[0:_KEYLEN_]
    fpath2=dir_paperreferences+"/"+key2+".txt"
    return [fpath1, fpath2]
    
# Function to compute co-authorship overlap
def compute_coauthorship_overlap(paper1_id, paper2_id):
    paper1_authors=[]
    paper2_authors=[]
    with open('chunk_swap','w+r') as f_chunk_swap:
        fpaths=get_paperauthoraffil_chunk_file_paths(paper1_id, paper2_id)
        fpath1=fpaths[0]
        fpath2=fpaths[1]
        if fpath1==fpath2:
            grep_process=subprocess.Popen(["grep","-h","-e",paper1_id,"-e",paper2_id, fpath1], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        else:
            grep_process=subprocess.Popen(["grep","-h","-e",paper1_id,"-e",paper2_id, fpath1, fpath2], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        grep_process.communicate()
        f_chunk_swap.seek(0)
        for line in f_chunk_swap:
            line_split=line.replace('\n','').replace('\r','').split('\t')
            paper_id=line_split[0]
            author_id=line_split[1]
            if (paper_id==paper1_id):
                paper1_authors.append(author_id)
            elif (paper_id==paper2_id):
                paper2_authors.append(author_id)
#    print("len(paper1_authors)="+str(len(paper1_authors))+",len(paper2_authors)="+str(len(paper2_authors)))
    # Find minimum of the number of authors of both papers
    min_authors=min(len(paper1_authors), len(paper2_authors))
    common_authors=0
    # Find the number of common authors
    for author1 in paper1_authors:
        for author2 in paper2_authors:
            if author1==author2:
                common_authors+=1
    # Remove the primary author from both the lists and reduce the min_authors count.
    common_authors-=1
    # This is because we want to exclude the very author(s) from the matching
    # which form(s) the basis for why we put these two papers as ambiguous.
#    paper1_authors.remove(paper1_author_id)
#    paper2_authors.remove(paper2_author_id)
    min_authors-=1
    if min_authors==0:
        result=0
    else:
        result=common_authors/min_authors
    #print("Co-authorship score: "+str(result))
    return result

# Function to compite self citation count
def compute_self_citation_count(paper1_id, paper2_id):
    pattern1=paper1_id+"\t"+paper2_id
    pattern2=paper2_id+"\t"+paper1_id
    fpaths=get_paperreferences_chunk_file_paths(paper1_id, paper2_id)
    fpath1=fpaths[0]
    fpath2=fpaths[1]
    grep_result=subprocess.Popen(["grep","-h","-e",paper1_id,"-e",paper2_id,fpath1,fpath2], shell=False, stdout=subprocess.PIPE).communicate()[0]
    result=(grep_result.find(pattern1) or grep_result.find(pattern2))
    return (1 if result!=-1 else 0)

# Function to compute shared reference count
def compute_shared_reference_count(paper1_id, paper2_id):
    paper1_references=[]
    paper2_references=[]
    with open('chunk_swap','w+r') as f_chunk_swap:
        fpaths=get_paperreferences_chunk_file_paths(paper1_id, paper2_id)
        fpath1=fpaths[0]
        fpath2=fpaths[1]
        if fpath1==fpath2:
            grep_process=subprocess.Popen(["grep","-h","-e",paper1_id,"-e",paper2_id,fpath1], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        else:
            grep_process=subprocess.Popen(["grep","-h","-e",paper1_id,"-e",paper2_id,fpath1,fpath2], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        grep_process.communicate()
        f_chunk_swap.seek(0)
        for line in f_chunk_swap:
            line_split=line.replace('\n','').replace('\r','').split('\t')
            paper_id=line_split[0]
            reference_id=line_split[1]
            if (paper_id==paper1_id):
                paper1_references.append(reference_id)
            elif (paper_id==paper2_id):
                paper2_references.append(reference_id)
#    print("len(paper1_references)="+str(len(paper1_references))+",len(paper2_references)="+str(len(paper2_references)))
    common_references=0
    for reference1 in paper1_references:
        for reference2 in paper2_references:
            if (reference1==reference2):
                common_references+=1
    #print("Shared reference count: "+str(common_references))
    return common_references

# Function to compute the citation overlap
def compute_citation_overlap(paper1_id, paper2_id):
    citation1=[]
    citation2=[]
    with open('chunk_swap','w+r') as f_chunk_swap:
        fpaths=get_paperreferences_chunk_file_paths(paper1_id, paper2_id)
        fpath1=fpaths[0]
        fpath2=fpaths[1]
        if fpath1==fpath2:
            grep_process=subprocess.Popen(["/bin/grep","-h","-e",paper1_id,"-e",paper2_id,fpath1], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        else:
            grep_process=subprocess.Popen(["/bin/grep","-h","-e",paper1_id,"-e",paper2_id,fpath1,fpath2], shell=False, stdout=f_chunk_swap)#subprocess.PIPE)
        grep_process.communicate()
        f_chunk_swap.seek(0)
        for line in f_chunk_swap:
            line_split=line.replace('\n','').replace('\r','').split('\t')
            paper_id=line_split[0]
            reference_id=line_split[1]
            if (reference_id==paper1_id):
                citation1.append(paper_id)
            elif (reference_id==paper2_id):
                citation2.append(paper_id)
#    print("len(citation1)="+str(len(citation1))+",len(citation2)="+str(len(citation2)))
    common_citations=0
    min_citations=min(len(citation1), len(citation2))
    for c1 in citation1:
        for c2 in citation2:
            if (c1==c2):
                common_citations+=1
    if min_citations==0:
        result=common_citations
    else:
        result=common_citations/min_citations
    #print("Common citations: "+str(result))
    return result

# Function to compute similarity score
def compute_similarity_score(paper1_id, paper2_id):
    t1=time.time()*1000
    # Compute co-authorship overlap
    coauthorship_overlap=compute_coauthorship_overlap(paper1_id, paper2_id)
    t2=time.time()*1000
    # Compute self citation count
    self_citation_count=compute_self_citation_count(paper1_id, paper2_id)
    t3=time.time()*1000
    # Compute shared reference count
    shared_reference_count=compute_shared_reference_count(paper1_id, paper2_id)
    t4=time.time()*1000
    # Compute citation overlap
#    citation_overlap=1
    citation_overlap=compute_citation_overlap(paper1_id, paper2_id)
    t5=time.time()*1000
    similarity_score=alpha_a*coauthorship_overlap+alpha_s*self_citation_count+alpha_r*shared_reference_count+alpha_c*citation_overlap
#    print("Similarity score: "+str(similarity_score)+", papers ["+paper1_id+","+paper2_id+"], coauthorship="+str(coauthorship_overlap)+",self_citation="+str(self_citation_count)+",shared_reference="+str(shared_reference_count)+",citation_overlap="+str(citation_overlap))
    print "Parameter Compuation Times (millis) [co-authorship, self-citation, shared reference, citation overlap]: [%s,%s,%s,%s]"%(t2-t1, t3-t2, t4-t3, t5-t4)
    return similarity_score

# Dictionary storing the similarity values of papers
paper_similarity={}

# Perform level 1 clustering i. e. form clusters of similar papers
def perform_clustering_l1():
    f_prevpos=open(path_prevpos,'r+w')
    nodes_compared=0
    nodes_added=0
    papers_verified=[]
    # Note the starting time
    t1=time.time()
    # Check for if there was any position up to which the papers were previously compared
    prevpos=f_prevpos.readline()
    if prevpos=='':
        prevpos_split=[]
    else:
        prevpos_split=prevpos.split('\t')
    logging.info('Previous positions: '+str(prevpos_split))
    # Start the comparison
    with codecs.open(path_authors, 'r', encoding='utf-8') as f_authors:
        logging.debug('Opened file '+path_authors)
        mmap_authors=mmap.mmap(f_authors.fileno(), 0, access=mmap.ACCESS_READ)
        # For each author from 'Authors.txt'
        for line in f_authors:
            line_split=line.replace('\n','').replace('\r','').split('\t')
            author_id=line_split[0]
            author_name=line_split[1]
            # Remember the current position that we are reading from
            pos=mmap_authors.tell()
            logging.debug('Working on Author: '+line_split[1])
            with open(path_paperauthoraffil, 'r') as f_paperauthoraffil_1:
                logging.debug('Opened file '+path_paperauthoraffil)
                # Set file pointer to point to previous position if exists
                if len(prevpos_split)!=0:
                    f_paperauthoraffil_1.seek(int(prevpos_split[0]))
                # For each paper author relationship
                for ppa_line1 in f_paperauthoraffil_1:
                    logging.debug('Reading paper author relationship: '+ppa_line1)
                    ppa_line1_split=ppa_line1.replace('\r', '').replace('\n', '').split('\t')
                    paper1_id=ppa_line1_split[0]
                    paper1_author_id=ppa_line1_split[1]
                    # Check for the author we are looking for
                    if(paper1_author_id!=author_id):
                        paper1_author_found=mmap_authors.find(paper1_author_id)
                        if (paper1_author_found != -1):
                            mmap_authors.seek(paper1_author_found)
                            paper1_author_name=mmap_authors.readline().replace('\n','').replace('\r','').split('\t')[1]
                            mmap_authors.seek(pos)
                            if (author_name != paper1_author_name):
                                continue
                        else:
                            continue
                    # print("First paper match found!")
                    with open(path_paperauthoraffil, 'r') as f_paperauthoraffil_2:
                        logging.debug('Opened file '+path_paperauthoraffil)
                        # Set file pointer to point to previous position if exists
                        if len(prevpos_split)!=0:
                            f_paperauthoraffil_2.seek(int(prevpos_split[1]))
                        prevpos_split=[]
                        # For each paper author relationship
                        for ppa_line2 in f_paperauthoraffil_2:
                            logging.debug('Reading paper author relationship: '+ppa_line2)
                            # Check if both the lines are the same
                            if(ppa_line2==ppa_line1):
                                continue
                            ppa_line2_split=ppa_line2.replace('\r', '').replace('\n','').split('\t')
                            paper2_id=ppa_line2_split[0]
                            paper2_author_id=ppa_line2_split[1]
                            # Check for the author we are looking for
                            # print("Comparing "+paper2_author_id+" and "+author_id)
                            if(paper2_author_id!=author_id):
                                paper2_author_found=mmap_authors.find(paper2_author_id)
                                if (paper2_author_found != -1):
                                    mmap_authors.seek(paper2_author_found)
                                    paper2_author_name=mmap_authors.readline().replace('\n','').replace('\r','').split('\t')[1]
                                    mmap_authors.seek(pos)
                                    if (author_name != paper2_author_name):
                                        continue
                                else:
                                    continue
                            # print("Second paper match found!")
                            if paper2_id in papers_verified:
                                # Paper already verified
                                continue
                            # Check if the similarity is already computed
                            if not paper_similarity.get(paper1_id):
                                logging.debug('Paper ['+paper1_id+'] was not compared with any paper.')
                                # Not computed with any paper
                                paper_similarity.update({paper1_id: {}})
                            simil_papers=paper_similarity.get(paper1_id)
                            if not simil_papers.get(paper2_id):
                                logging.debug('Paper ['+paper1_id+'] was not compared with paper ['+paper2_id+']')
                                # Compute similarity score
                                similarity_score=compute_similarity_score(paper1_id, paper2_id)
                                # Update the similarity score
                                simil_papers.update({paper2_id: similarity_score})
#                                logging.debug('Updated similarity score for paper ['+paper1_id+'] - paper ['+paper2_id+'] as ['+str(similarity_score)+']')
                                # Check if similarity score is above the threshold
                                nodes_compared+=1
                                if (similarity_score>=beta_1):
                                    papers_verified.append(paper2_id)
                                    # Add paper 2 to the cluster of paper 1
                                    # The following query finds if a cluster exists for paper 1, creates it if it does not and then adds paper 2 to the cluster of paper 1
                                    add_to_cluster_query="MERGE (cl1:ClusterL1)<-[:BELONGS_TO]-(p:PaperID{pid: '"+paper1_id+"'}) CREATE (cl1)<-[:BELONGS_TO]-(p1:PaperID{pid: '"+paper2_id+"'})"
                                    qresult=cypher_resource.execute(add_to_cluster_query)
                                    nodes_added+=1
#                                    logging.info("Added paper ID "+paper2_id+" to the cluster of paper ID "+paper1_id)
#                                    if(paper1_author_id!=paper2_author_id):
#                                        print("Different Authors: "),
#                                        print("["+paper1_id+" ("+paper1_author_id+"),"+paper2_id+" ("+paper2_author_id+")] Paper similarity score: "+str(similarity_score))
                                else:
#                                    logging.info("Similarity score for paper ID "+paper2_id+" and "+paper1_id+" is "+similarity_score+", less than "+beta_1)
                                    pass
                            f_prevpos.truncate(0)
                            f_prevpos.seek(0)
                            f_prevpos.write(str(f_paperauthoraffil_1.tell())+'\t'+str(f_paperauthoraffil_2.tell()))
                                #print("["+paper1_id+","+paper2_id+"] Similarity score: "+str(similarity_score)+" Individual attributes: ["+str(coauthorship_overlap)+","+str(self_citation_count)+","+str(shared_reference_count)+","+str(citation_overlap)+"]")
#                                print("["+paper1_id+","+paper2_id+"] Paper similarity score: "+str(similarity_score))
    f_prevpos.close()
    # Note the completion time
    t2=time.time()
    t_l1=round(t2-t1, 2)
    logging.info("Total time taken for level 1 clustering: "+str(t_l1)+" second(s)")
    print("Nodes compared: "+str(nodes_compared)+" Nodes added: "+str(nodes_added))
    print("Size of array 'papers_verified': "+str(sys.getsizeof(papers_verified)))
    return

# Perform clustering of level 2 i. e. form clusters of similar clusters
def perform_clustering_l2():
    f_cluster_stats=open('cluster_stats.txt','w')
    t1=time.time()
    cluster_paper_records={}
    # Compute cluster similarities
    cluster_similarity={}
    verified_clusters=[]
    get_clusters_query="MATCH (c:ClusterL1) RETURN ID(c) ORDER BY ID(c)"
    get_papers_query="MATCH (c:ClusterL1)<-[r:BELONGS_TO]-(p:PaperID) WHERE ID(c)=_CLUSTER_ID_ RETURN p.pid ORDER BY ID(p)"
    cluster_records=cypher_resource.execute(get_clusters_query)
    clusters_size=len(cluster_records)
    cluster1_index=0
    # Clusters- Outer loop
    for cluster1_record in enumerate(cluster_records):
        cluster1_index+=1
        cluster1=cluster1_record.__getitem__(1).__getitem__(0)
        if(cluster1==None):
            continue
#        if cluster1 in verified_clusters:
#            continue
        # Clusters- Inner loop
        cluster2_index=0
        for cluster2_record in enumerate(cluster_records):
            cluster2_index+=1
            cluster2=cluster2_record.__getitem__(1).__getitem__(0)
            if(cluster2==None):
                continue
            if(cluster2==cluster1):
                continue
            if cluster2 in verified_clusters:
                continue
            if cluster1 not in cluster_paper_records:
                cluster1_paper_records=cypher_resource.execute(get_papers_query.replace('_CLUSTER_ID_', str(cluster1)))
                cluster_paper_records[cluster1]=cluster1_paper_records
            else:
                cluster1_paper_records=cluster_paper_records[cluster1]
            cluster1_size=len(cluster1_paper_records)
            cluster2_size=0
            # Papers- Outer loop
            paper1_index=0
            cluster_similarity_score=0
            for paper1_record in cluster1_paper_records:
                paper1_index+=1
                paper1=str(paper1_record.__getitem__(0))
                if(paper1==None):
                    continue
                if cluster2 not in cluster_paper_records:
                    cluster2_paper_records=cypher_resource.execute(get_papers_query.replace('_CLUSTER_ID_', str(cluster2)))
                    cluster_paper_records[cluster2]=cluster2_paper_records
                else:
                    cluster2_paper_records=cluster_paper_records[cluster2]
                cluster2_size=len(cluster2_paper_records)
                # Papers- Inner loop
                paper2_index=0
                for paper2_record in cluster2_paper_records:
                    paper2_index+=1
                    paper2=str(paper2_record.__getitem__(0))
                    if(paper2==None):
                        continue
                    simil_papers=paper_similarity.get(paper1)
                    similarity_score=0
                    if not simil_papers:
                        similarity_score=compute_similarity_score(str(paper1), str(paper2))
                    else:
                        similarity_score=simil_papers.get(paper2)
                        if not similarity_score:
                            similarity_score=compute_similarity_score(str(paper1), str(paper2))
                    if similarity_score>beta_2:
                        # Add to similarity score of the clusters
                        cluster_similarity_score+=similarity_score
                    print("[%s/%s %s/%s %s/%s %s/%s]" % (cluster1_index, clusters_size, cluster2_index, clusters_size, paper1_index, cluster1_size, paper2_index, cluster2_size))
            if (cluster1_size!=0 and cluster2_size!=0):
                cluster_similarity_score/=(cluster1_size*cluster2_size)
                f_cluster_stats.write(str(cluster_similarity_score)+"\t"+str(cluster1)+"\t"+str(cluster2)+"\n")
                if not cluster_similarity.get(cluster1):
                    cluster_similarity.update({cluster1: {}})
                simil_cluster=cluster_similarity.get(cluster1)
                if not simil_cluster.get(cluster2):
                    simil_cluster.update({cluster2: cluster_similarity_score})
                if cluster_similarity_score>beta_3:
                    add_to_cluster_cluster_query="MATCH (c1:ClusterL1) WITH c1 WHERE ID(c1)="+str(cluster1)+" MERGE (c2:ClusterL2)<-[:BELONGS_TO]-(c1) WITH c2 MATCH (c1a:ClusterL1) WITH c2,c1a WHERE ID(c1a)="+str(cluster2)+" CREATE (c2)<-[:BELONGS_TO]-(c1a)"
                    qresult=cypher_resource.execute(add_to_cluster_cluster_query)
                    if cluster1 not in verified_clusters:
                        verified_clusters.append(cluster1)
                    verified_clusters.append(cluster2)
    print(cluster_similarity)
    f_cluster_stats.close()
    # Note the completion time
    t2=time.time()
    time_taken=str(round(t2-t1, 2))+" second(s)"
    logging.info("Total time taken for level 2 clustering: "+time_taken)
    notification_text="The level 2 clustering of the algorithm has completed.\n\n"\
        "Time elapsed: %s\n" % (time_taken)
    notifier.send_notification(notification_sender,notification_receivers,notification_subject, notification_text)
    return

# Function to start the algorithm i. e. level 1 clustering
def start_algo():
    buckets={}
    papers_verified=[]
    t1=time.time()
    total_lines=subprocess.check_output(["grep", "-vc", "'^$'", path_paperauthoraffil]).replace('\n','')
    print("Total papers: "+str(total_lines))
    average_bucketing_time=0
    f_prevpos=open(path_prevpos, 'r')
    prevpos=0
    try:
        prevpos=long(f_prevpos.readline().replace('\n',''))
    except Exception as e:
        print("Exception occurred!")
        print(e)
        prevpos=0
    f_prevpos.close()
    print("prevpos=["+str(prevpos)+"]")
    first_run=True
    f_prevpos=open(path_prevpos, 'a')
    with open(path_paperauthoraffil, 'r') as f_paperauthoraffil:
        with codecs.open(path_authors, 'r', 'utf-8') as f_authors:
            mmap_authors=mmap.mmap(f_authors.fileno(), 0, access=mmap.ACCESS_READ)
            lnum=1
            for line_paperauthoraffil in f_paperauthoraffil:
                ta1=time.time()*1000
                print("["+str(lnum)+"/"+str(total_lines)+"] "),
                lnum+=1
                if first_run:
                    if (lnum-1)<=prevpos:
                        continue
                    else:
                        # Wrte to file
                        f_prevpos.truncate(0)
                        f_prevpos.seek(0)
                        f_prevpos.write(str(lnum-1))
                        first_run=False
                else:
                    # Wrte to file
                    f_prevpos.truncate(0)
                    f_prevpos.seek(0)
                    f_prevpos.write(str(lnum-1))
                line_split=line_paperauthoraffil.replace('\n','').replace('\r','').split('\t')
                paper_id=line_split[0]
                author_id=line_split[1]
                mmap_authors.seek(0)
                index_authors=mmap_authors.find(author_id)
                if index_authors != -1:
                    mmap_authors.seek(index_authors)
                    author_name=mmap_authors.readline().replace('\n','').replace('\r','').split('\t')[1].encode('utf-8')
                    if author_name not in buckets:
                        buckets[author_name]=[]
                    for paperx_id in buckets.get(author_name):
                        if paper_id in papers_verified:
                            # Paper already verified
                            continue
                        # Check if the similarity is already computed
                        if not paper_similarity.get(paperx_id):
                            logging.debug('Paper ['+paperx_id+'] was not compared with any paper.')
                            # Not computed with any paper
                            paper_similarity.update({paperx_id: {}})
                        simil_papers=paper_similarity.get(paperx_id)
                        if not simil_papers.get(paper_id):
                            logging.debug('Paper ['+paperx_id+'] was not compared with paper ['+paper_id+']')
                            # Compute similarity score
                            similarity_score=compute_similarity_score(paperx_id, paper_id)
                            # Update the similarity score
                            simil_papers.update({paper_id: similarity_score})
                            #                                logging.debug('Updated similarity score for paper ['+paper1_id+'] - paper ['+paper2_id+'] as ['+str(similarity_score)+']')
                            # Check if similarity score is above the threshold
                            if (similarity_score>=beta_1):
                                papers_verified.append(paper_id)
                                # Add paper 2 to the cluster of paper 1
                                # The following query finds if a cluster exists for paper 1, creates it if it does not and then adds paper 2 to the cluster of paper 1
                                add_to_cluster_query="MERGE (cl1:ClusterL1{author_name: '"+author_name+"'})<-[:BELONGS_TO]-(p:PaperID{pid: '"+paperx_id+"'}) CREATE (cl1)<-[:BELONGS_TO]-(p1:PaperID{pid: '"+paper_id+"'})"
                                qresult=cypher_resource.execute(add_to_cluster_query)
                                #                                    logging.info("Added paper ID "+paper2_id+" to the cluster of paper ID "+paper1_id)
                                #                                    if(paper1_author_id!=paper2_author_id):
                                #                                        print("Different Authors: "),
                                #                                        print("["+paper1_id+" ("+paper1_author_id+"),"+paper2_id+" ("+paper2_author_id+")] Paper similarity score: "+str(similarity_score))
                            else:
                                #                                    logging.info("Similarity score for paper ID "+paper2_id+" and "+paper1_id+" is "+similarity_score+", less than "+beta_1)
                                pass
#                            print("["+paperx_id+","+paper_id+"] Paper similarity score: "+str(similarity_score)),
                    buckets[author_name].append(paper_id)

                    ta2=time.time()*1000
                    time_taken=round(ta2-ta1, 2)
                    average_bucketing_time+=time_taken
                    print("in "+str(time_taken)+" ms")
    average_bucketing_time/=int(total_lines)
    print("Size of buckets: "+str(sys.getsizeof(buckets))+" bytes\nBuckets:")
    print(buckets)
    t2=time.time()
    f_prevpos.close()
    with open('bucketed_authors.txt','w') as f_bucketed_authors:
        for key, value in buckets.iteritems():
            f_bucketed_authors.write(key+"\n")
    time_taken=str(round(t2-t1, 2))+" second(s)"
    print("Time taken for bucketing Authors: "+time_taken)
    notification_text="The level 1 clustering of the algorithm has completed.\n\n"\
        "Time elapsed: %s\n"\
        "Buckets count: %s\n"\
        "Average bucketing time: %d ms" % (time_taken, str(len(buckets)), average_bucketing_time)
    notifier.send_notification(notification_sender,notification_receivers,notification_subject, notification_text)
    return

def main():
    start_algo()
    perform_clustering_l2()

main()
