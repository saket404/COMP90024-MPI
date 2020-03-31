from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter
import getopt,sys,json,re,logging
import time

test = ['st0','st1','st2','st3','st5','st6','st7','st8'] * 1000000


start_time = time.time()
c_1 = Counter(['st0','st1','st2','st3','st5','st6','st7','st8'])
for i in test:
    c_1.update(i)
total = time.time() - start_time
mins,secs = divmod(total,60)
print(f'Total Time for Execution is {mins} minutes and {secs} seconds')

print('\n\n')

start_time = time.time()
c_1 = Counter(['st0','st1','st2','st3','st5','st6','st7','st8'])
for i in test:
    c_1[i] += 1
total = time.time() - start_time
mins,secs = divmod(total,60)
print(f'Total Time for Execution is {mins} minutes and {secs} seconds')


print('\n\n')

start_time = time.time()
c_2 = Counter(['st0','st1','st2','st3','st5','st6','st7','st8'])
c_2.update(test)
total = time.time() - start_time
mins,secs = divmod(total,60)
print(f'Total Time for Execution is {mins} minutes and {secs} seconds')