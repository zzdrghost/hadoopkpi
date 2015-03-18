# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 15:24:12 2015
Create a xml document
@author: ZHEN
"""

from xml.dom.minidom import Document

doc=Document()
graph=doc.createElement('graph')
graph.setAttribute('caption','24小时访问量变化情况')
graph.setAttribute('xAxisName','time')
graph.setAttribute('yAxisName','pv')
graph.setAttribute('baseFontSize','12')
doc.appendChild(graph)

def addFCset(newFCset):
    FCset=doc.createElement('set')
    FCset.setAttribute('label',newFCset['label'])
    FCset.setAttribute('value',newFCset['value'])
    graph.appendChild(FCset)

for time,pv in zip(Time,Pv):
    
    addFCset({'label':str(time.day)+'日'+str(time.hour)+'时','value':str(pv)})

print doc.toprettyxml()
f=file("FCdata.xml","w")
doc.writexml(f)
f.close()  