# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import community
import networkx as nx
import os.path
import time
import matplotlib.pyplot as plt
from random import choice
import operator
import multiprocessing
import itertools

# <codecell>

BASE_PATH = "/home/nmtsylla/python_labs/projet_sna/graph/"
graph = nx.read_adjlist('/home/nmtsylla/python_labs/projet_sna/out.cfinder-google')
liste = []
cpte = 0
centralities = {}
betweennesses = {}

# <codecell>

def findPartitionL(G):
    partition = community.best_partition(G)
    size = float(len(set(partition.values())))
    print size, len (partition.values())
    count = 0
    fileList =[]
    for com in set(partition.values()) :
        count = count + 1
        list_nodes = [nodes for nodes in partition.keys()
                                if partition[nodes] == com]
       
        if len (list_nodes) > 250 :
            n = nx.subgraph(G,list_nodes)
            filename= BASE_PATH+'Partition'+ str(count) +'.txt'
            nx.write_adjlist(n, filename, '#')
            fileList.append(filename)
    return fileList

# <codecell>

def findPartitionK(G):
    partition = list (nx.k_clique_communities(G, 3))
    print len (partition)
    print partition [1]
    print partition [2]
    print len(partition [0])
cpte_centrality = 0
cpte_betweeness = 0
cpte_centrality = 0

# <codecell>

def centrality_degree(g, n):
    print multiprocessing.current_process().name
    g = nx.read_adjlist(g)
    dc = nx.degree_centrality(g)
    fileName = open(BASE_PATH+"centrality_"+str(n)+".txt", "w")
    nx.set_node_attributes(g, 'degree_cent', dc)
    degcent_sorted = sorted(dc.items(), key = operator.itemgetter(1), reverse = True)
    for key, value in enumerate(degcent_sorted):
        fileName.write("\nNode "+str(value[0])+" centrality degree  is: " + str(value[1]))
    fileName.close()
    centrest = "Le noeud le plus central de cette partition est: " + str(tuple(reversed(degcent_sorted[0]))[1])
    return dc, centrest

# <codecell>

def betweenness(g, n):
    print multiprocessing.current_process().name
    g = nx.read_adjlist(g)
    dc = nx.betweenness_centrality(g)
    fileName = open(BASE_PATH+"betweenness_"+str(n)+".txt", "w")
    nx.set_node_attributes(g, 'betweenness', dc)
    degcent_sorted = sorted(dc.items(), key = operator.itemgetter(1), reverse = True)
    for key, value in enumerate(degcent_sorted):
        fileName.write("\nNode "+str(value[0])+" betweenness degree  is: " + str(value[1]))
    fileName.close()
    print g
    return dc

# <codecell>

def degree_distrib(graph, n):
    g = nx.read_adjlist(graph)
    dc = nx.degree_histogram(g)
    fileName = open(BASE_PATH+"histogram_"+str(n)+".txt", "w")
    nx.set_node_attributes(g, 'degree_cent', dc)
    #degcent_sorted = sorted(dc.items(), key = operator.itemgetter(1), reverse = True)
    for key, value in enumerate(dc):
        fileName.write("\nThe degree " + str(key) + " frequency is "+ str(value))
        fileName.close()
        return dc

# <codecell>

liste = findPartitionL(graph)
def all_centrality(liste):
    cpte = 0
    for g in liste:
        cpte += 1
        liste, noeud_central = centrality_degree(g, cpte)
        print noeud_central
        
def all_distrib_degree(liste):
    cpte = 0
    for g in liste:
        cpte += 1
        degree_distrib(g, cpte)
        
def all_betweenness(liste):
    cpte = 0
    for g in liste:
        cpte += 1
        betweenness(g, cpte)

# <codecell>

di = multiprocessing.Process(target=all_centrality, args=[liste], name="betweeness distrib process")
p = multiprocessing.Process(target=all_distrib_degree, args=[liste], name="centrality process")
q = multiprocessing.Process(target=all_betweenness, args=[liste], name="betweenness process")
di.start()
p.start()
q.start()
p.join()
di.join()
q.join()

# <codecell>

d = betweenness(nx.read_adjlist("/home/nmtsylla/python_labs/projet_sna/graph/Partition2.txt"), 300)

# <codecell>

start = time.time()
c = 0
print "Sequentiel version"
all_betweenness(liste)
print("\t\tTime: %.4F" % (time.time()-start))
print "-----------------------------"
print "Parallel version"
start = time.time()
p = multiprocessing.Process(target=all_betweenness, args=[liste])
p.start()
p.join()
print "\t\tTime: %.4F" % (time.time()-start)

# <codecell>

def betweenn(g):
    print multiprocessing.current_process().name
    g = nx.read_adjlist(g)
    dc = nx.betweenness_centrality(g)
    fileName = open(BASE_PATH+"betweenness.txt", "w")
    nx.set_node_attributes(g, 'betweenness', dc)
    degcent_sorted = sorted(dc.items(), key = operator.itemgetter(1), reverse = True)
    for key, value in enumerate(degcent_sorted):
        fileName.write("\nNode "+str(value[0])+" betweenness degree  is: " + str(value[1]))
    fileName.close()
    return dc
start = time.time()
pool = multiprocessing.Pool(processes=8)
pool.map(betweenn, liste)
pool.close()
pool.join()
print("\t\tTime: %.4F" % (time.time()-start))

# <codecell>


