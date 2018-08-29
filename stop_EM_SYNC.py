#!/usr/bin/python
#_*_ coding:utf-8 _*_
'''
Created on 2018年2月24日
Function: 
@author: Jessica
'''

import os
from ConfigParser import ConfigParser

def GetPidfile():
    ConfFile = 'EM_SYNC.conf'
    if not os.path.isfile(ConfFile):
        raise ValueError('Not found ConfFile %s' % ConfFile)
    config = ConfigParser()
    config.read(ConfFile)
    pidfile = config.get('pidfile', 'file')
    return pidfile

def stop(pidfile):
    with open(pidfile,'r+b') as f:
        for i in f.readlines():
            pid = i.strip()
            if pid: os.popen('taskkill /F /PID %s' % pid)
    with open(pidfile,'w+b'): pass
    
if __name__ == '__main__':
    stop(GetPidfile())

        
