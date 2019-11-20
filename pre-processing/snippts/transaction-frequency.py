#!/usr/bin/env python3
from blockchain_parser.blockchain import Blockchain
import time


blockchain_path = "/Volumes/C/selective-blocks/"
blockchain = Blockchain(blockchain_path)
start_time2 = time.time()
print("Processing...")
nodes_count = 0
node_count_list = []
years = [2011, 2012, 2013, 2014]

for block in blockchain.get_unordered_blocks():
    nodes_count = 0
    if block.header.timestamp.year in years:
        new_file = blockchain.selected_blk_file
        for tx in block.transactions:
            nodes_count = nodes_count + 1
    node_count_list.append(nodes_count)
