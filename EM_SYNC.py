#!/usr/bin/python
#_*_ coding:utf-8 _*_
'''
Created on 2017年12月25日
Function: Sync Files
@author: Jessica
'''

import os,sys,time,md5,logging,json,socket,select,string
from multiprocessing import Process,Queue,Pipe
from ConfigParser import ConfigParser
from threading import Thread,Lock

def RecordOps(level, msg, AbsLog):
    LogPath= os.path.split(AbsLog)[0]
    if not os.path.isdir(LogPath):os.makedirs(LogPath)
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG,
                        filename=AbsLog + '_' + time.strftime('%Y%m%d') + '.log',
                        filemode='a',)
    LevelTypes = ['notset','debug','info','warning','error','critical']
    assert level in LevelTypes,'Invalid log level %s.' % level
    eval('logging.%s'% level )(msg)

def Md5File(filename, log):
    try:
        _file = open(filename,'r+b')
    except IOError,e:
        if 'Permission denied' in str(e):
            md5Value = False
            RecordOps('error', e, log)
        else:
            md5Value = 'null'
    else:
        filedata = _file.read()
        md5Obj = md5.new()
        md5Obj.update(filedata)
        md5Value = md5Obj.hexdigest()
        _file.close()
        
    return md5Value

def GenPidfile(AbsPidfile, mode='w+b'):
    path = os.path.dirname(AbsPidfile)
    if not os.path.isdir(path): os.makedirs(path)
    with open(AbsPidfile, mode) as f:
        f.write(str(os.getpid())+os.linesep)
    
class Inotify(Process):
    
    def __init__(self, que, pip, log, pidfile, interval, waittime):
        super(Inotify, self).__init__()
        self.que = que
        self.pip = pip
        self.dict = {}
        self.log = log
        self.pidfile = pidfile
        self.interval = interval
        self.waittime = waittime
    
    def GenInfo(self, fileobj, event):
        self.dict[fileobj] = {}
        self.dict[fileobj]['md5val'] = Md5File(fileobj, self.log)
        self.dict[fileobj]['filename'] = fileobj
        self.dict[fileobj]['event'] = event
        
    def ProcComm(self, event):
        while True:
            self.que.put(event)
            excpt = self.pip.recv()
            if not excpt:
                break
    
    def AddDel(self, action, FilesSeq):
        for item in FilesSeq:
            self.GenInfo(item, action)
            if self.dict[item]['md5val']:
                self.ProcComm(self.dict[item])
        if action == 'DELETE':
            del self.dict[item]
    
    def modify(self, CurFiles):
        for item in CurFiles:
            NewMd5Val = Md5File(item, self.log)
            if NewMd5Val != 'null' and NewMd5Val != self.dict[item]['md5val']:
                self.dict[item]['md5val'] = NewMd5Val
                self.dict[item]['event'] = 'MODIFY'
                self.ProcComm(self.dict[item])

    def InitSync(self):
        self.AddDel('INIT', os.listdir('.'))

    def listen(self):
        while True:
            OldRecords = set(os.listdir('.'))
            for item in OldRecords: self.GenInfo(item, None)
            time.sleep(self.interval)
            while True:
                NewRecords = set(os.listdir('.'))
                AddRecords = NewRecords - OldRecords
                DelRecords = OldRecords - NewRecords
                if AddRecords or DelRecords:
                    if AddRecords:
                        self.AddDel('ADD', AddRecords)
                    if DelRecords:
                        self.AddDel('DELETE', DelRecords)
                    break
                self.modify(NewRecords)
                time.sleep(self.interval)
                
    def run(self):
        GenPidfile(self.pidfile)
        self.InitSync()
        self.listen()

class SyncClient(Process):
    
    def __init__(self, ipaddr, port, que, pip, log, pidfile):
        super(SyncClient, self).__init__()
        self.ipaddr = ipaddr
        self.port = port
        self.que = que
        self.pip = pip
        self.log = log
        self.pidfile = pidfile
    
    def CreatSession(self, ip, port):
        SvrsIp = ip if isinstance(ip, list) else [ip,]
        sessions = []
        for i in SvrsIp:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((i, port))
            except socket.error,e:
                errmsg = 'Build sync session failed with %s<%s>!' % (i, e)
                RecordOps('error', errmsg, self.log)
            else:
                sessions.append(s)
                errmsg = 'Build sync session success with %s!' % i
                RecordOps('info', errmsg, self.log)
            sys.stderr.write(errmsg + os.linesep)
        return sessions
    
    # 文件数据分片   
    def SliceData(self, filename, filesize=None):
        try:
            if not filesize: filesize = os.path.getsize(filename)
            with open(filename, 'r+b') as filedata:
                TranseredSize = 0
                while not TranseredSize >= filesize:
                    RestedSize = filesize - TranseredSize
                    if RestedSize > 1024:
                        data = filedata.read(1024)
                        TranseredSize += 1024
                    else:
                        # 补全数据包长度处理粘包（使用' '做字节填充）
                        data = filedata.read(RestedSize) + ' '*(1024 - RestedSize)
                        TranseredSize += RestedSize
                    yield data
        except Exception,e:
            RecordOps('error', e, self.log)
            return
    
    def SeriaData(self, OriData):
        SrzData = json.dumps(OriData)
        SrzSize = len(SrzData)
        if SrzSize > 1024:
            errmsg = 'Event <%s> length has overflowed!' % OriData
            RecordOps('Critical', errmsg, self.log)
            raise ValueError(errmsg)
        else:
            SrzData += ' ' * (1024 - SrzSize)
        return SrzData
    
    def SendThrd(self, EvtMsg, sock, lck):
        try:
            sock.send(self.SeriaData(EvtMsg))
            flag = True
            if self.SliceData(EvtMsg['filename'], EvtMsg['filesize']):
                for piece in self.SliceData(EvtMsg['filename']):
                    sock.send(piece)
                RecordOps('info', 'transfered %s success!' % EvtMsg['filename'], self.log)
            else:
                flag = False
        except socket.error, e:
            self.sessions.remove(sock)
            RecordOps('error', e, self.log)
        else:
            lck.acquire()
            self.rets.append(flag)
            lck.release()
    
    def SendData(self):
        self.sessions = self.CreatSession(self.ipaddr, self.port)
        _lock = Lock()
        while True:
            EvtMsg = self.que.get()
            # 调试print '---->',EvtMsg 
            if EvtMsg['event'] in ['INIT', 'ADD', 'MODIFY']:
                EvtMsg['filesize'] = os.path.getsize(EvtMsg['filename'])
            else:
                EvtMsg['filesize'] = None
            self.rets = []
            # 发送文件数据
            for s in self.sessions:
                ChdThrd = Thread(target=self.SendThrd, args=(EvtMsg, s, _lock))
                ChdThrd.start()
            while len(self.sessions) != len(self.rets): time.sleep(0.0001)
            self.pip.send('') if all(self.rets) else self.pip.send(' ')
    
    def run(self, *args, **kwargs):
        time.sleep(0.01)
        GenPidfile(self.pidfile, mode='a+b')
        self.SendData()

def RunClient(SrcDir, ipaddr , port, log, pidfile, QueSize, interval, waittime):
    os.chdir(SrcDir)
    ProcQue = Queue(QueSize)
    ProcPip = Pipe(duplex=True) 
    # 生产进程
    ProdProc = Inotify(ProcQue, ProcPip[0], log, pidfile, interval, waittime)
    # 消费进程
    ConsProc = SyncClient(ipaddr, port, ProcQue, ProcPip[1], log, pidfile)
    ProdProc.daemon = True
    ConsProc.daemon = True
    print 'Listening ',SrcDir,' ...'
    ProdProc.start()
    ConsProc.start()
    ProdProc.join()
    ConsProc.join()

class SyncServer():
    
    def __init__(self, ipaddr, port, DstDir, log, pidfile):
        self.ipaddr = ipaddr
        self.port = port
        self.DstDir = DstDir
        self.log = log
        self.pidfile = pidfile
    
    def ParaseSerData(self, sock, filledstr=' '):
        OrdData = False
        while not OrdData:
            data = sock.recv(1024)
            VaildData = data.strip(filledstr)
            try:
                OrdData = json.loads(VaildData)
            except ValueError:
                self.AddOvfwData(self.LastFilename, data)
                errmsg = 'serialized %s failed.' % data
                RecordOps('error', errmsg, self.log)
        return OrdData

    def SpliceData(self, sock, filename, filesize):
        try:
            with open(filename, 'w+b') as _file:
                RecvedSize = 0
                # 本地计算数据包的填充字节数
                filledsize = 1024 - (filesize % 1024)
                SumSize = filledsize + filesize
                while not RecvedSize >= filesize:
                    data = sock.recv(1024)
                    if SumSize - RecvedSize == 1024:
                        _file.write(data[:-filledsize])
                        RecvedSize += len(data) - filledsize
                    else:
                        _file.write(data)
                        RecvedSize += len(data)
            RecordOps('info', '%s received success.' % filename, self.log)
        except Exception,e:
            RecordOps('error', 'Splicedata() <%s>.' % e, self.log)
        return None
    
    def delete(self, filename):
        try:
            os.remove(filename)
        except WindowsError,e:
            RecordOps('error', e, self.log)
    
    def AddOvfwData(self, filename, data):
        try:
            with open(filename,'a+b') as _file:
                _file.write(data)
        except IOError,e:
            errmsg = 'overflowed <%s> adds to %s failed!(%s).' % (data, filename, e)
            RecordOps('error', errmsg, self.log)
            
    def ServeForever(self):
        GenPidfile(self.pidfile)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.ipaddr, self.port))
        sock.listen(5)
        inputs = [sock]
        while True:
            rs, ws, es = select.select(inputs, [], [])
            for s in rs:
                if s is sock:
                    CliSock, ipport = sock.accept()
                    inputs.append(CliSock)
                    RecordOps('info', 'recv conn from %s:%s.' % ipport, self.log)
                else:
                    try:
                        EvtMsg = self.ParaseSerData(s)
                        if EvtMsg['event'] in ['INIT', 'ADD', 'MODIFY']:
                            self.SpliceData(s, EvtMsg['filename'], EvtMsg['filesize'],)
                            self.LastFilename = EvtMsg['filename']
                        elif EvtMsg['event'] == 'DELETE':
                            self.delete(EvtMsg['filename'])
                        else:
                            RecordOps('error', 'unsupport event %s.' % EvtMsg, self.log)
                    except socket.error:
                        inputs.remove(s)

def RunServer(ipaddr, port, DstDir, log, pidfile):
    svr = SyncServer(ipaddr, port, DstDir, log, pidfile)
    os.chdir(DstDir)
    print 'Listening %s:%s' % (ipaddr, port),'...'
    svr.ServeForever()
    
if __name__ == '__main__':
    ConfFile = 'EM_SYNC.conf'
    if not os.path.isfile(ConfFile):
        raise ValueError('Not found ConfFile %s' % ConfFile)
    config = ConfigParser()
    config.read(ConfFile)
    role = config.get('role', 'role')
    ipaddr = config.get('ipaddr', 'ipaddr')
    port = config.getint('port', 'port')
    srcdir = config.get('srcdir', 'src')
    dstdir = config.get('dstdir', 'dst')
    logfile = config.get('logfile', 'log')
    pidfile = config.get('pidfile', 'file')
    size = config.getint('queuesize', 'size')
    waittime = config.getfloat('waittime', 'time')
    interval = config.getfloat('interval', 'interval')
    
    if string.lower(role) == 'server':
        RunServer(ipaddr, port, dstdir, logfile, pidfile)
    elif string.lower(role) == 'client':
        ipaddr = ipaddr.split(',')
        RunClient(srcdir, ipaddr, port, logfile, pidfile, size, interval, waittime)
    else:
        raise ValueError('unsupport role %s, expect server or client!' % role)
    
 
