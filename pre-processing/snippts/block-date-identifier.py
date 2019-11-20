from blockchain_parser.blockchain import Blockchain
import time


start_time = time.time()
path = "/Volumes/C/bitcoin-blockchain/blockchain/blocks/"
start_file = path+'blk00000.dat'
#years = [2011, 2012, 2013, 2014]
years = [2011, 2012]
processed_years = set()

blockchain = Blockchain(path)

for block in blockchain.get_unordered_blocks():
    if block.header.timestamp.year in years:        
        new_file = blockchain.selected_blk_file
        if new_file != start_file or block.header.timestamp.year not in processed_years:
            processed_years.add(block.header.timestamp.year)
            start_file = new_file
            print("File: "+new_file+"   Year:"+str(block.header.timestamp.year))


print("--- %s seconds ---" % (time.time() - start_time))