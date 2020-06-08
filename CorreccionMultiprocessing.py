#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 09:37:09 2020

@author: Do
"""

import time
import numpy as np
import functools
import multiprocessing
from multiprocessing import Process, Manager
import copy

def how_many_within_range_sequential(row, minimum, maximum):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count += 1
    return count

def how_many_within_range_parallel(arr):
    count = 0
    for n in arr:
        if 4 <= n <= 8:
            count += 1
    return count

if __name__ == '__main__':
    np.random.RandomState(100)
    arr = np.random.randint(0, 10, size=[4000000, 10])
    ar = arr.tolist()
    inicioSec = time.time()
    resultsSec = []
    for row in ar:
        resultsSec.append(how_many_within_range_sequential(row, minimum=4, maximum=8))
    finSec = time.time()
    resultsSec.sort()
    
    
    inicioPar = time.time()
    pool = multiprocessing.Pool(processes=20)
    resultsPar = pool.map(how_many_within_range_parallel, ar)
    pool.close() 
    pool.join()  
    finPar = time.time()
    resultsPar.sort()
    
    
    print('Results are correct!\n' if functools.reduce(lambda x, y : x and y, map(lambda p, q: p == q,resultsSec,resultsPar), True) else 'Results are incorrect!\n')
    print('Sequential Process took %.3f ms \n' % ((finSec - inicioSec)*1000))
    print('Parallel Process took %.3f ms \n' % ((finPar - inicioPar)*1000))
    
    
    
    
    
    