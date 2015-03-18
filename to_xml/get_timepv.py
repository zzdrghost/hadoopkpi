# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:18:24 2015

@author: ZHEN
"""
from datetime import datetime

ftest=open("E:/pyexc/part-r-00000","r")
time_pv=ftest.readlines()
time=[]
pv=[]
for value in time_pv:
    time.append(str.split(value)[0])
    pv.append(str.split(value)[1])
Time=[]
for value in time:
    Time.append(datetime(year=int(value[0:4]),
                         month=int(value[4:6]),
                        day=int(value[6:8]),
                        hour=int(value[8:10])))
Pv=[]
for value in pv:
    Pv.append(int(value))
del pv,time,value
