#!/usr/bin/python3
import time
from time import mktime
from datetime import datetime
import gc
import networkx as nx
import sys



def bytes_2_human_readable(number_of_bytes):
    if number_of_bytes < 0:
        raise ValueError("!!! number_of_bytes can't be smaller than 0 !!!")

    step_to_greater_unit = 1024.

    number_of_bytes = float(number_of_bytes)
    unit = 'bytes'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'KB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'MB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'GB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'TB'

    precision = 1
    number_of_bytes = round(number_of_bytes, precision)

    return str(number_of_bytes) + ' ' + unit


def avg_transaction_time_interval(times):
    try:
        if len(times) > 1:
            times_list = sorted((time.strptime(d, "%Y-%m-%d %H:%M:%S") for d in times), reverse=False)
            d_diff = 0
        
            for i in range(1,len(times_list)):
                d0 = datetime.fromtimestamp(mktime(times_list[i-1]))
                d1 = datetime.fromtimestamp(mktime(times_list[i]))
                d_diff = d_diff + (d1-d0).total_seconds()
            
            avg_diff = d_diff/(len(times_list)-1)
        else:
            avg_diff = 0
    
        return avg_diff
    except:
        print(times)
        return 0



def load_graph_from_file(path):
    start_time = time.time()
    G = nx.read_gml(path)
    print("Reading Graph Minutes: "+str(((time.time() - start_time)/60.0)))  
    return G



def write_graph_to_file(G, path):
    start_time = time.time()
    nx.write_gml(G, path+"graph.gml")
    print("Writing Graph Minutes: "+str(((time.time() - start_time)/60.0)))  

   
    
    
def extract_data_from_graph(G, path):
    start_time = time.time()
    out_file = open(path+"graph_out.csv", "w")
    out_file.write("tx_hash,indegree,outdegree,in_btc,out_btc,total_btc,mean_in_btc,mean_out_btc,avg_interval_in_tx,avg_interval_out_tx,is_anomaly\n");
    #total_nodes = len(G.nodes())
    #print('\n\n')
    #count = 1
    for u in G.nodes():
        #print(str(count)+' of '+str(total_nodes)+' written!')
        in_btc = 0.0
        mean_in_btc = 0.0
        
        out_btc = 0.0
        mean_out_btc = 0.0
        
        indegree = G.in_degree(u)
        outdegree = G.out_degree(u)
        
        avg_interval_in_tx_lst = []
        avg_interval_out_tx_lst = []
        
        isAnomaly = 0
        
        for e1,e2,a in G.in_edges(u, data=True):
            in_btc = in_btc + float(a['btc'])
            avg_interval_in_tx_lst.append(a['date'])
            if a['anomaly'] == 'True':
                isAnomaly = 1

        avg_interval_in_tx = avg_transaction_time_interval(avg_interval_in_tx_lst)
        
        
        if indegree != 0:
            mean_in_btc = in_btc/indegree
        
        for e1,e2,a in G.out_edges(u, data=True):
            out_btc = out_btc + float(a['btc'])
            avg_interval_out_tx_lst.append(a['date'])
            if a['anomaly'] == 'True':
                isAnomaly = 1
                
        avg_interval_out_tx = avg_transaction_time_interval(avg_interval_out_tx_lst)

            
        if outdegree != 0:    
            mean_out_btc = out_btc/outdegree
        
# =============================================================================
#         print(str(indegree) + "," + str(outdegree) + "," + str(in_btc) + "," + str(out_btc) + "," + 
#               str((in_btc+out_btc)) + "," + str(mean_in_btc) + "," + str(mean_out_btc) + "," +
#               str(avg_interval_in_tx) + "," + str(avg_interval_out_tx) + "," + str(isAnomaly))
# =============================================================================
            
        out_file.write(str(u) + "," + str(indegree) + "," + str(outdegree) + "," + str(in_btc) + "," + str(out_btc) + "," + 
              str((in_btc+out_btc)) + "," + str(mean_in_btc) + "," + str(mean_out_btc) + "," +
              str(avg_interval_in_tx) + "," + str(avg_interval_out_tx) + "," + str(isAnomaly)+"\n")
        
        #count = count + 1
    print("Extraction Time Minutes: "+str(((time.time() - start_time)/60.0)))  


if __name__ == "__main__":
    gc.collect()  
    start_time2 = time.time()
    path = "/Users/omershafiq/Documents/Thesis/2011_2013_c/"
    G = nx.DiGraph()
    print("Processing...")

    with open(path+'out.csv') as infile:
        for line in infile:
            if line is not '':
                in_hash, out_hash, tx_time, value, status = line.split(',')
                if status == True:
                    print("YES")
                if status == 'True\n':
                    print("OK")
                    
                #G.add_edge(str(in_hash), out_hash, date=tx_time, btc=value, anomaly=status)
                
    print("\n\nTotal Time Minutes: "+str(((time.time() - start_time2)/60.0)))        
    #extract_data_from_graph(G, path)
    print("Checkpoint-1")
    #write_graph_to_file(G, path)
    print("Ended!")

