#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 10:48:35 2020

@author: Do
"""

from mpi4py import MPI
import numpy as np
import time
import functools

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

numworkers = size - 1

if rank == 0:
    def how_many_within_range_sequential(row, minimum, maximum):
        count = 0
        for n in row:
            if minimum <= n <= maximum:
                count += 1
        return count
    
    np.random.RandomState(100)
    arr = np.random.randint(0, 10, size=[4000000, 10])
    ar = arr.tolist()
    #print(arr)
    
    
    inicioSec = time.time()
    resultsSec = []
    for row in ar:
        resultsSec.append(how_many_within_range_sequential(row, minimum=4, maximum=8))
    finSec = time.time()
    resultsSec.sort()
    #print(resultsSec)
    
    filas = np.size(arr,0) // numworkers
    residuo = np.size(arr,0) % numworkers
    inicio = 0
    
    inicioPar = time.time()
    for i in range(1, size):
        numFilas = filas+1 if i <= residuo else filas
        fin = inicio + numFilas
        comm.send(arr[inicio:fin], dest=i, tag=1)
        inicio += numFilas
    resultsPar = []   
    for i in range(1,size):
        res = comm.recv(source=i, tag=1)
        resultsPar += res
    finPar = time.time()
    resultsPar.sort()
    #print(resultsPar)
    
    print('Results are correct!\n' if functools.reduce(lambda x, y : x and y, map(lambda p, q: p == q,resultsSec,resultsPar), True) else 'Results are incorrect!\n')
    print('Sequential Process took %.3f ms \n' % ((finSec - inicioSec)*1000))
    print('Parallel Process took %.3f ms \n' % ((finPar - inicioPar)*1000))
      
if rank > 0:
    arr = comm.recv(source=0, tag=1).tolist()
    minimum = 4
    maximum = 8
    
    res = []
    for row in arr:
        count = 0
        for n in row:
            if minimum <= n <= maximum:
                count += 1
        res.append(count)
  
    comm.send(res, dest=0, tag=1)







    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    