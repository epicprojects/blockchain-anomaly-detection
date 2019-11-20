#!/usr/bin/python3
import time
import gc


if __name__ == "__main__":
    gc.collect()  
    start_time2 = time.time()
    path = "/Volumes/C/output/"  
    print("Processing...")    
    i = 0
    with open(path+'out2.csv') as infile:
        for line in infile:
            i = i + 1;
           
    print("\n\nTotal Time Minutes: "+str(((time.time() - start_time2)/60.0)))        
    print("Ended!")
    print('Total Records: '+str(i))

