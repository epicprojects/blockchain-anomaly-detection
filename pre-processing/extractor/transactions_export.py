#!/usr/bin/python3
from blockchain_parser.blockchain import Blockchain
import time
import json
import gc
import hashlib





def writeToFile(dic1, dic2, count):
     start_time = time.time()
     
     for key, value in abandoned_d2.items():
         if key in dic1:
             sender_hash, index = key.split(':')
             receiver_hash = str(dic1[key])
             jObj = json.loads(str(value).replace("'", '"'))
             value = str(jObj['value'])
             tx_time = str(jObj['time'])
             tx_malicious = str(jObj['malicious'])
             #node_in_id = str(int(hashlib.sha256(str(sender_hash).encode('utf-8')).hexdigest(), 16) % (10 ** 10))
             #node_out_id = str(int(hashlib.sha256(receiver_hash.encode('utf-8')).hexdigest(), 16) % (10 ** 10))
             out_file.write(str(sender_hash) + "," + receiver_hash + "," + tx_time + "," + value + "," + tx_malicious + "\n") 
             found_keys.append(key)
             
     for key, value in dict2.items():                      
          if key in dic1:
             sender_hash, index = key.split(':')
             receiver_hash = str(dic1[key])
             jObj = json.loads(str(value).replace("'", '"'))
             value = jObj['value']
             tx_time = jObj['time']
             tx_malicious = str(jObj['malicious'])
             #node_in_id = str(int(hashlib.sha256(str(sender_hash).encode('utf-8')).hexdigest(), 16) % (10 ** 10))
             #node_out_id = str(int(hashlib.sha256(receiver_hash.encode('utf-8')).hexdigest(), 16) % (10 ** 10))
             out_file.write(str(sender_hash) + "," + receiver_hash + "," + tx_time + "," + value + "," + tx_malicious + "\n")                           
          else:
             abandoned_d2[key] = value  


     print("Preprocessing Minutes: "+str(((time.time() - start_time)/60.0)))  
     start_time = time.time()
     dic1.clear()
     dic2.clear()
     [abandoned_d2.pop(k, None) for k in found_keys]
     found_keys.clear()
     dict1.clear()
     dict2.clear()







if __name__ == "__main__":
    gc.collect()
    
    #years = [2011, 2012, 2013, 2014]
    years = [2011, 2012, 2013]
    satoshi = 0.00000001
    
    path = "/Volumes/C/"
    out_file = open(path+"btc_graph_2011_2013_r/out.csv", "w")
    
    loss_tx_data = path + "anomalies-data/anomalies_loss_tx.csv"
    loss_txs = [line.strip() for line in open(loss_tx_data, 'r')]
    
    misc_tx_data = path + "anomalies-data/anomalies_misc_tx.csv"
    misc_tx = [line.strip() for line in open(misc_tx_data, 'r')]
    
    seziure_tx1_data = path + "anomalies-data/anomalies_seizure1_tx.csv"
    seziure_tx1 = [line.strip() for line in open(seziure_tx1_data, 'r')]
    
    seziure_tx2_data = path + "anomalies-data/anomalies_seizure2_tx.csv"
    seziure_tx2 = [line.strip() for line in open(seziure_tx2_data, 'r')]
    
    theft_tx_data = path + "anomalies-data/anomalies_theft_tx.csv"
    theft_tx = [line.strip() for line in open(theft_tx_data, 'r')]
    
    anomalies = loss_txs + misc_tx + seziure_tx1 + seziure_tx2 + theft_tx
    

    blockchain_path = path+'selective-blocks/'

    start_file = blockchain_path+'blk00000.dat'
    
      
    abandoned_d2 = dict()
    found_keys = list()
    dict1 = dict()
    dict2 = dict()
    count = 0

    blockchain = Blockchain(blockchain_path)
    start_time2 = time.time()
    print("Processing...")
    for block in blockchain.get_unordered_blocks():
        if block.header.timestamp.year in years:
            new_file = blockchain.selected_blk_file
            for tx in block.transactions:
                anomaly_flag = False
                if str(tx.hash) in anomalies:
                    anomaly_flag = True
                try:
                    for in_no, input in enumerate(tx.inputs):
                         key = str(input.transaction_hash)+':'+str(input.transaction_index)
                         dict1[key] = tx.hash
                    for out_no, output in enumerate(tx.outputs):
                        address = "not-spent"
                        if (len(output.addresses) != 0):
                            address = output.addresses
                            key = str(tx.hash)+':'+str(out_no)
                            dict2[key] = {'value': str(output.value*satoshi), 'time': str(block.header.timestamp), 'malicious': str(anomaly_flag)}
                except Exception:
                    print("There was a hiccup with: "+ str(tx.hash))
                    pass
                
            if new_file != start_file:
                print("Blk File Processing Minutes: "+str(((time.time() - start_time2)/60.0)))
                writeToFile(dict1, dict2, count)
                start_file = new_file
                count = count + 1
                print(str(count) + " File(s) Processed!")
                start_time2 = time.time()
                
    print("\n")        
    print(str(count+1) + " File(s) Processed!")        
    print("Blk File Processing Minutes: "+str(((time.time() - start_time2)/60.0)))        
    writeToFile(dict1, dict2, count)
    print("Ended!")
    gc.collect()
    out_file.close()


