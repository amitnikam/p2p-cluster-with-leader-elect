### DEFAULT PYTHON 3.8.3 MODULES
import socket
import threading
import pickle
import argparse
import logging
import time
import os
import hashlib
import math
import concurrent.futures

### Code to Pass Arguments to Server Script through Linux Terminal
parser = argparse.ArgumentParser(description = "This is a distributed node in the P2P Architecture!")
parser.add_argument('--ip', metavar = 'ip', type = str, nargs = '?', default = socket.gethostbyname(socket.gethostname()))
parser.add_argument('--port', metavar = 'port', type = int, nargs = '?', default = 9000)
parser.add_argument('--dir', metavar = 'dir', type = str, nargs = '?', default = './hosted_files')
parser.add_argument('-t', metavar = 't', type = bool, nargs = '?', default = False)
args = parser.parse_args()

### MAKE DIRECTORY TO LOG OUTPUTS TO(IF NOT MADE)
if not os.path.exists('./logs'):
    os.makedirs('./logs')
    print(f'{"[SETUP]":<26}./logs directory created.')

### MAKE DIRECTORY TO HOST FILES FROM(IF NOT MADE)
dir_loc = f'{args.dir}/{args.port}'
if not os.path.exists(dir_loc):
    os.makedirs(dir_loc)
    print(f'{"[SETUP]":<26}{dir_loc} directory created. Keep files which you want to host here.')

### SETUP LOGGING WITH DYNAMIC NAME
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(message)s')
file_handler = logging.FileHandler(f'./logs/Node-{args.port}.log')
file_handler.setFormatter(formatter)
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)
logger.addHandler(file_handler)


### CONNECTION PROTOCOL & ADDRESSES
HEADER = 16                  # Size of header
PACKET = 2048                # Size of a packet, multiple packets are sent if message is larger than packet size. 
FORMAT = 'utf-8'             # Message format
ADDR = (args.ip, args.port)  # Address socket server will bind to
TOTAL_CONN = 0               # Current connections 
CHUNK_SIZE = 1536             # Size of file chunks
LEADER = False               # Leader Status
LEADER_TIME = None           # Record leader time
DHT_ADDR = None              # Address of DHT Node
NODE_LIST = []               # List of Nodes in Network
dht = {}                     # To save DHT Record (if leader)
dht_sec = {}                 # To save DHT Record (if leader)
TEST_START = False           # For testing purpose
LAST_CHECK = 0               # For DHT TIMELY CHECKS
TOTAL_UP = 0                 # For Bandwidth Test
TOTAL_DOWN = 0               # For Bandwidth test

### DEFAULT MESSAGES
REQ_FILE_LIST_MESSAGE = "!FILE_LIST"
RES_FILE_LIST_MESSAGE = "!RES_FILE_LIST"
REQ_FILE_SRC_MESSAGE = "!REQ_FILE_SRC_MESSAGE"
RES_FILE_SRC_MESSAGE = "!RES_FILE_SRC_MESSAGE"
DOWNLOAD_MESSAGE = "!DOWNLOAD"
RES_DOWNLOAD_MESSAGE = "!RES_DOWNLOAD"
DISCONNECT_MESSAGE = "!DISCONNECT"
LEADER_CHECK = "!LEADER_CHECK"
RES_LEADER_CHECK = "!RES_LEADER_CHECK"
UPDATE_LEADER = "!UPDATE_LEADER"
UPDATE_DHT = "!UPDATE_DHT"
RES_UPDATE_DHT = "!RES_UPDATE_DHT"
DEACTIVE_NODE = "!DEACTIVE_NODE"
TEST_MESSAGE = "!TEST_MESSAGE"
REQ_META_DATA = "!REQ_META_DATA"
RES_META_DATA = "!RES_META_DATA"
REQ_CHK_FILE = "!REQ_CHK_FILE"
RES_CHK_FILE = "!RES_CHK_FILE"

### DISTRIBUTED HASH TABLE (ONLY USED WHEN LEADER)
class DHT:
    
    ## CONSTRUCTOR MAKE TO LISTS
    def __init__(self):
        self.data = {}
        self.data_second = {}
    
    ## RETURN SOURCES FOR A FILE AND UPDATE MAYBE LIST OF POSSIBLE FILE SOURCES
    def sourceList(self, addr, file_name):
        self.sec = None
        if addr not in self.data[file_name]:
            if file_name in self.data_second.keys():
                self.sec = self.data_second[file_name]
                if addr not in self.data_second[file_name]:
                    self.data_second[file_name].append(addr)
            else:
                self.data_second[file_name] = []
                self.data_second[file_name].append(addr)
        return (self.data[file_name], self.sec)

    ## RETURN COMPLETE LIST OF FILES
    def fileList(self):
        file_list = [] 
        for key in self.data.keys(): 
            file_list.append(key)
        return file_list

    ## UPDATE A NODE IN DHT RECORD
    def update(self, addr, file_list):
        for f in file_list:
            if f in self.data.keys(): 
                if addr not in self.data[f]:
                    self.data[f].append(addr)
            else:
                self.data[f] = []
                self.data[f].append(addr)
            
            if f in self.data_second.keys(): 
                if addr in self.data_second[f]:
                    self.data_second[f].remove(addr)

    ## DELETE NODE FROM DHT RECORD
    def delete(self, addr):
        chk = []
        for f in self.data:
            if addr in self.data[f]:
                self.data[f].remove(addr)
                chk.append(f)
        for f in chk:
            if not self.data[f]:
                self.data.pop(f)

### CONNECTION HANDLER THREAD
class ConnThread(threading.Thread):

    ## CONSTRUCTOR (THREAD A NEW OR EXISTING CONNECTION)
    def __init__(self, conn=None, addr=DHT_ADDR, track=False):
        threading.Thread.__init__(self)
        # Track active connections
        self.track = track
        if self.track == True:
            global TOTAL_CONN
            TOTAL_CONN += 1
        
        # Outward Connection
        self.addr = addr
        if conn is None:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.settimeout(3)
            self.conn.connect(self.addr)
            self.conn.settimeout(None)
            logger.info(f'{"[NEW CONNECTION OUT]":<26}{self.addr}')
        
        # Inward Connection
        else:
            self.conn = conn
            logger.info(f'{"[NEW CONNECTION IN]":<26}{self.addr}')
        
        # HANDLER BUFFER PARAMETERS
        self.listen = True
        self.buffer_file_list = None
        self.buffer_file_srcs = None
        self.buffer_file_data = None
        self.buffer_down_size = None
        self.buffer_meta_data = None
        self.buffer_check_file = None
        self.buffer_leader_check = None
        self.buffer_update_dht_status = None
        

    ##
    ### BASIC FUNCTIONS
    ##

    ## SEND MESSAGE FUNCTION
    def send(self,msg):
        # message pickled into bytes and HEADER added to message
        msg = pickle.dumps(msg)
        msg = bytes(f'{len(msg):<{HEADER}}', FORMAT) + msg
        global TOTAL_UP
        TOTAL_UP += len(msg)
        if len(msg) > PACKET:
            for i in range(0, len(msg), PACKET):
                self.conn.send(msg[i:i+PACKET])
            return len(msg)
        self.conn.send(msg)
        return len(msg)

    ## FUNCTION TO GET FILE LIST FROM LOCAL HOSTED DIRECTORY
    def localFileList(self):
        dir_loc = f'{args.dir}/{args.port}/'
        return [f for f in os.listdir(dir_loc) if os.path.isfile(os.path.join(dir_loc, f))]

    ## FUNCTION TO SAFELY DISCONNECT AND CLOSE CONNECTION
    def disconnect(self):
        self.listen = False
        msg = {'main':DISCONNECT_MESSAGE}
        self.send(msg)
        self.conn.close()
        logger.info(f'{"[DISCONNECTED]":<26}{self.addr}')

    ##
    ### DHT SERVER FUNCTIONS
    ##
    
    ## CHECK IF A NODE IS LEADER
    def leaderPing(self):
        global ADDR
        msg = {'main':LEADER_CHECK,'addr':ADDR}
        self.send(msg)
        while self.buffer_leader_check is None:
            time.sleep(.1)
        if self.buffer_leader_check:
            global DHT_ADDR
            DHT_ADDR = self.addr
            return True
        else:
            return False

    ## UPDATE LEADER ON OTHER NODE
    def updateLeader(self):
        msg = {'main':UPDATE_LEADER, 'addr':ADDR}
        self.send(msg)
        logger.info(f'{"[LEADER ELEC NOTICE]":<26}{self.addr}')

    ## FUNCTION TO ADD NODE ON TO DHT 
    def updateDHT(self):
        msg = {'main':UPDATE_DHT, 'addr':ADDR, 'file_list':self.localFileList()}
        self.send(msg)
        logger.info(f'{"[ADDING SELF TO DHT]":<26}')
        while not self.buffer_update_dht_status:
            time.sleep(.1)
        temp = self.buffer_update_dht_status
        self.buffer_update_dht_status = None
        return temp

    ## FUNCTION TO REMOVE NODE FROM DHT
    def removeFromDHT(self):
        msg = {'main':DEACTIVE_NODE, 'addr':ADDR}
        self.send(msg)
        logger.info(f'{"[REMOVE SELF FROM DHT]":<26}')

    ## FUNCTION TO GET FILE LIST FROM DHT
    def getFileList(self):
        self.send({'main':REQ_FILE_LIST_MESSAGE})
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_file_list is None:
            time.sleep(.1)
        f_list = self.buffer_file_list
        self.buffer_file_list = None
        return f_list

    ## FUNCTION TO GET NODES THAT CAN PROVIDE THE FILE
    def getFileSources(self, fname):
        self.send({'main':REQ_FILE_SRC_MESSAGE, 'addr':ADDR, 'file_name':fname})
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_file_srcs is None:
            time.sleep(.1)
        s_list = self.buffer_file_srcs
        self.buffer_file_srcs = None
        return s_list
    

    ##
    ### INTER NODE FUNCTIONS
    ##

    ## FUNCTION TO GET META DATA OF A FILE
    def fileMeta(self, fname):
        self.send({'main':REQ_META_DATA, 'addr':ADDR, 'file_name':fname})
        logger.info(f'{"[FETCH META DATA]":<26}For {fname}')
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_meta_data is None:
            time.sleep(.1)
        meta = self.buffer_meta_data
        self.buffer_meta_data = None
        return meta

    ## FUNCTION TO CHECK CHUNKS AT A NODE
    def checkChunks(self, fname):
        self.send({'main':REQ_CHK_FILE, 'addr':ADDR, 'file_name':fname})
        logger.info(f'{"[CHECK SOURCE]":<26}For {fname} chunks')
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_check_file is None:
            time.sleep(.1)
        chk = self.buffer_check_file
        self.buffer_check_file = None
        return chk

    ## FUNCTION TO DOWNLOAD FILE CHUNK FROM REMOTE NODE
    def downloadChunk(self, d, cnumber):
        down_file_time = time.time()
        self.send({'main':DOWNLOAD_MESSAGE,'file_name':d,'cnumber':cnumber})
        # RESPONSE RECEIVE
        while self.buffer_file_data is None:
            time.sleep(.0005)
        # PROCEED IF RIGHT RESPONSE
        if self.buffer_file_data['file_name'] == d and self.buffer_file_data['cnumber'] == cnumber:
            # GENERATE LOCAL MD5 FOR CHUNK
            md5_mirror = hashlib.md5(self.buffer_file_data['chunk_data']).hexdigest()
            # RETURN IF INTEGRITY CHECK SUCCESSFUL AND REPORT STATS
            if self.buffer_file_data['md5'] == md5_mirror:
                down_file_time = time.time()-down_file_time
                logger.info(f'{"[DOWNLOAD INFO]":<26}{d}#{cnumber} downloaded from {self.addr}')
                logger.info(f'{"[DOWNLOAD STAT]":<26}{self.buffer_down_size} Bytes <- {self.addr} in {down_file_time} Seconds')
                print(f'\n{d}#{cnumber}\nmd5: {md5_mirror}\nIntegrity check pass, downloaded successfully!\nDownloaded in {down_file_time} seconds')
                payload = self.buffer_file_data['chunk_data']
                self.buffer_file_data = None
                self.buffer_down_size = None
                return (True, payload)
            # DON'T SAVE IF INTEGRITY CHECK FAILS, TRY AGAIN LATER
            else:
                print(f'\n{d}#{cnumber}\nIntegrity failures.')
                self.buffer_file_data = None
                self.buffer_down_size = None
                return (False, None)
        else:
            self.buffer_file_data = None
            self.buffer_down_size = None
            return (False, None)

    ##
    ### RECEIVER
    ##

    def run(self):
        while self.listen:
            msg = {'main':''}
            
            # RECEIVE MESSAGE HEADER > GET LENGTH OF MESSAGE > SAVE AND DECODE FULL MESSAGE
            msg_length = self.conn.recv(HEADER)
            if not msg_length:
                msg['main'] = DISCONNECT_MESSAGE
            
            if msg_length:
                # Start the process only for a valid header 
                full_msg = b''
                new_msg = True
                # loop to download full message body
                while True:
                    # receive message packets
                    msg = self.conn.recv(PACKET)
                    
                    # get length from header
                    if new_msg:
                        msg_len = int(msg_length)
                        full_msg = msg_length
                        new_msg = False
                    
                    full_msg += msg

                    # decode and break out of loop if full message is received
                    if len(full_msg)-HEADER == msg_len:
                        global TOTAL_DOWN
                        TOTAL_DOWN += len(full_msg) 
                        msg = pickle.loads(full_msg[HEADER:])
                        break
            
            ## UPDATE DHT RECORD
            if msg['main'] == UPDATE_DHT:
                global LEADER

                # INFORM IF NOT LEADER
                if not LEADER:
                    res = {'main':RES_UPDATE_DHT, 'status':LEADER}
                    self.send(res)
                # UPDATE DHT AND ACK(IF LEADER)
                else:
                    global dht
                    dht.update(msg['addr'],msg['file_list'])
                    res = {'main':RES_UPDATE_DHT, 'status':LEADER}
                    self.send(res)
                    logger.info(f'{"[DHT UPDATED BY]":<26}{msg["addr"]}')

            ## RESPOND TO UPDATE DHT RECORD REQUEST
            if msg['main'] == RES_UPDATE_DHT:
                # SUCCESSFUL UPDATE
                if msg['status']:
                    logger.info(f'{"[DHT UPDATE DONE]":<26}')
                    self.buffer_update_dht_status = True
                
                # WRONG UPDATE ATTEMPT
                else:
                    logger.info(f'{"[DHT UPDATE FAILED]":<26}')
                    self.buffer_update_dht_status = False

            ## UPDATE LEADER AT OTHER NODE
            if msg['main'] == UPDATE_LEADER:
                global DHT_ADDR
                DHT_ADDR = msg['addr']
                logger.info(f'{"[NEW LEADER ELECTED]":<26}{DHT_ADDR}')
                while True:
                    try:
                        dht_conn = ConnThread(addr=DHT_ADDR, track=False)
                        f_list = False
                        while not f_list:
                            f_list = dht_conn.updateDHT()
                        dht_conn.disconnect()
                        break
                    except:
                        findDHT()

            # RESPOND TO LEADER CHECK
            if msg['main'] == LEADER_CHECK:
                res = {'main':RES_LEADER_CHECK, 'leader':LEADER}
                self.send(res)
                logger.info(f'{"[LEADER PING]":<26}{msg["addr"]}')
                if msg['addr'] not in NODE_LIST:
                    NODE_LIST.append(msg['addr'])
                    logger.info(f'{"[NODES DISCOVERED]":<26}{len(NODE_LIST)} Node(s) in Network')


            # HAND RESPONSE FOR LEADER CHECK
            if msg['main'] == RES_LEADER_CHECK:
                self.buffer_leader_check = msg['leader']
                logger.info(f'{"[LEADER PING]":<26}{self.addr}')

            # CASE: REQ FOR FILE LIST, SEND DHT FILE LIST
            if msg['main'] == REQ_FILE_LIST_MESSAGE:
                if LEADER:
                    res = {'main':RES_FILE_LIST_MESSAGE, 'status':LEADER, 'file_list':dht.fileList()}
                    logger.info(f'{"[FILE LIST REQ]":<26}')
                    self.send(res)
                else:
                    res = {'main':RES_FILE_LIST_MESSAGE, 'status':LEADER}
                    logger.info(f'{"[WRONG FILE LIST REQ]":<26}')
                    self.send(res)
            
            # CASE: RES FOR A FILE LIST REQUEST, SAVE IN BUFFER & HANDLE FAILURE
            if msg['main'] == RES_FILE_LIST_MESSAGE:
                if msg['status']:
                    self.buffer_file_list = msg['file_list']
                    logger.info(f'{"[FILE LIST RECEIVED]":<26}')
                else:
                    self.buffer_file_list = False
                    logger.info(f'{"[WRONG DHT NODE]":<26}')

            # CASE: REQ FOR FILE SOURCES, SEND DHT FILE SOURCES
            if msg['main'] == REQ_FILE_SRC_MESSAGE:
                if LEADER:
                    primary, secondary = dht.sourceList(msg['addr'], msg['file_name'])
                    res = {'main':RES_FILE_SRC_MESSAGE, 'status':LEADER, 'src_list':primary, 'src_list_sec':secondary}
                    logger.info(f'{"[FILE SOURCES REQ]":<26}')
                    self.send(res)
                else:
                    res = {'main':RES_FILE_SRC_MESSAGE, 'status':LEADER}
                    logger.info(f'{"[WRONG FILE SOURCES REQ]":<26}')
                    self.send(res)
            
            # CASE: RES FOR A FILE LIST REQUEST, SAVE IN BUFFER & HANDLE FAILURE
            if msg['main'] == RES_FILE_SRC_MESSAGE:
                if msg['status']:
                    self.buffer_file_srcs = (msg['src_list'],msg['src_list_sec'])
                    logger.info(f'{"[FILE SOURCES RECEIVED]":<26}')
                else:
                    self.buffer_file_srcs = False
                    logger.info(f'{"[WRONG DHT NODE]":<26}')
            
            # REMOVE NODE FROM DHT
            if msg['main'] == DEACTIVE_NODE:
                dht.delete(msg['addr'])
                logger.info(f'{"[NODE REMOVED FROM DHT]":<26}{msg["addr"]}')

            # REQUESTING META DATA FOR A FILE
            if msg['main'] == REQ_META_DATA:
                dir_loc = f'{args.dir}/{args.port}/'
                file_name = os.path.join(dir_loc, msg['file_name'])
                file_data = open(file_name,'rb').read()
                res = {'main':RES_META_DATA, 'fname':msg['file_name'], 'fsize':len(file_data), 'chunks':math.ceil(len(file_data)/CHUNK_SIZE)}
                logger.info(f'{"[FILE META DATA REQ]":<26}From {msg["addr"]}')
                self.send(res)

            # RESPONSE TO META DATA REQUEST
            if msg['main'] == RES_META_DATA:
                logger.info(f'{"[META DATA RECEIVED]":<26}From {self.addr}')
                self.buffer_meta_data = {'fname':msg['fname'], 'fsize':msg['fsize'], 'chunks':msg['chunks']}

            # MESSAGE TO CHECK THE CHUNKS AT A NODE
            if msg['main'] == REQ_CHK_FILE:
                flist = self.localFileList()
                logger.info(f'{"[FILE CHUNKS CHECK]":<26}From {msg["addr"]}')
                # commits only if file available
                if msg['file_name'] in flist:
                    res = {'main':RES_CHK_FILE, 'status': True, 'file_name':msg['file_name']}
                    self.send(res)
                else:
                    res = {'main':RES_CHK_FILE, 'status': False, 'file_name':msg['file_name']}
                    self.send(res)

            # RESPONSE FOR CHECKING THE CHUNKS REQUEST
            if msg['main'] == RES_CHK_FILE:
                self.buffer_check_file = msg['status']

            # CASE: DOWNLOAD REQUEST
            if msg['main'] == DOWNLOAD_MESSAGE:
                up_time = time.time()
                # FIND FILE
                dir_loc = f'{args.dir}/{args.port}/'
                file_name = os.path.join(dir_loc, msg['file_name'])
                file_open = open(file_name,'rb')
                file_data = file_open.read()
                # FIND SPECIFIC CHUNK IN THE FILE
                cstart = msg['cnumber'] * CHUNK_SIZE
                cend = (msg['cnumber']+1) * CHUNK_SIZE
                chunk = file_data[cstart:cend]
                # GENERATE MD5
                md5 = hashlib.md5(chunk).hexdigest()
                # SEND MD5 AND CHUNK BINARY DATA
                res = {'main':RES_DOWNLOAD_MESSAGE, 'file_name':msg['file_name'], 'md5':md5, 'chunk_data':chunk, 'cnumber': msg['cnumber']}
                up_size = self.send(res)
                # REPORT THE UPLOAD STATS
                up_time = time.time()-up_time
                logger.info(f'{"[UPLOAD INFO]":<26}{msg["file_name"]}#{msg["cnumber"]} sent to {self.addr}')
                logger.info(f'{"[UPLOAD STAT]":<26}{up_size} Bytes -> {self.addr} in {up_time} Seconds')

            # CASE: RES FOR DOWNLOAD REQUEST, SAVE TO BUFFER
            if msg['main'] == RES_DOWNLOAD_MESSAGE:
                self.buffer_down_size = len(full_msg)
                self.buffer_file_data = msg

            ## MESSAGE TO START THE TEST
            if msg['main'] == TEST_MESSAGE:
                global TEST_START
                TEST_START = True

            # CASE: DISCONNECTING REMOTE NODE, RELEASE CONNECTION
            if msg['main'] == DISCONNECT_MESSAGE:
                logger.info(f'{"[DISCONNECT ACK]":<26}{self.addr}')
                if self.track == True:
                    global TOTAL_CONN
                    TOTAL_CONN -= 1
                    logger.info(f'{"[ACTIVE CONNECTIONS]":<26}{TOTAL_CONN}')
                    # if leader and connection 0, check for new leader in network.
                    if TOTAL_CONN == 0 and LEADER == True and (time.time() - LAST_CHECK) > 5:
                        findDHT()
                self.listen = False
                self.conn.close()

### SELECT SPECIFIC FILES FROM A LIST OF FILES TO DOWNLOAD
def selectFileFromList(file_list):
    ## DISPLAY LIST
    print("\nSelect file to download by index number.\n")
    print(f'{"Index":<8}{"File Name":<20}')
    for f in file_list:
        print(f'{file_list.index(f):<8}{f:<20}')
    ##  SAVE USER INPUT
    li = int(input('\n'))
    ## SHOW THE FILE THAT WILL BE DOWNLOADED
    print(f'\nSelected {file_list[li]}\n')
    return file_list[li]

### HANDLE DOWNLOAD OF FILE AT HIGHER LEVEL. TAKES FILE NAME AND SELECTED SOURCE LIST AS INPUT.
def downloadHandler(fl, slist):
    logger.info(f'{"[DOWNLOAD HANDLER START]":<26}')
    
    ## CHECK SOURCES FOR FILE CHUNKS
    primary = []
    for s in slist:
        ## GET META DATA FOR FILE
        src_conn = ConnThread(addr=s)
        src_conn.start()
        chunks_val = src_conn.checkChunks(fl)
        src_conn.disconnect()
        if chunks_val:
            primary.append(s)
    
    ### START DOWNLOAD USING WINDOWED ROUND ROBIN
    
    ## GET META DATA FOR FILE
    meta_conn = ConnThread(addr=primary[0])
    meta_conn.start()
    file_meta_data = meta_conn.fileMeta(fl)
    meta_conn.disconnect()

    ## ALGORITHM TO DECIDE WHERE TO DOWNLOAD CHUNKS FROM 
    available_srcs = len(primary)
    down_chunks = [0 for _ in range(available_srcs+1)]
    down_chunks[1] = math.floor(file_meta_data['chunks']/available_srcs) + file_meta_data['chunks']%available_srcs
    if available_srcs > 1:
        for i in range(2,available_srcs+1):
            down_chunks[i] = down_chunks[i-1] + math.floor(file_meta_data['chunks']/available_srcs)
    
    ## DISPLAY DOWNLOAD SOURCES
    print(f'\nTotal chunks: {file_meta_data["chunks"]}. Downloading using Windowed Round-Robin.')
    for i in range(1,available_srcs+1):
        print(f'From: {primary[i-1]} downloading: {down_chunks[i-1]} - {down_chunks[i]} chunks')
    if not TEST_START:
        time.sleep(5)

    ## START DOWNLOAD
    down_start_time = time.time()
    # To reconstruct file, make an array for binary data
    complete_data_array = [b'' for _ in range(available_srcs)]
    # Thread(1st degree) the parallel connections to concurrently download chunks from different nodes
    with concurrent.futures.ThreadPoolExecutor() as executor:
        threads = [executor.submit(downloadFrom, i, primary[i], fl, down_chunks[i],down_chunks[i+1]) for i in range(available_srcs)]
        # as soon as any download ends, take action
        for down in concurrent.futures.as_completed(threads):
            index, chunk_data = down.result()
            complete_data_array[index] = chunk_data

    ## RECONSTRUCT FILE
    complete_data = b''
    for fblock in complete_data_array:
        complete_data += fblock
    
    ## SAVE FILE
    dir_loc = f'{args.dir}/{args.port}/'
    file_mirror = open(os.path.join(dir_loc,fl), 'wb')
    file_mirror.write(complete_data)
    file_mirror.close()

    ## COMPLETION
    print(f'\nDownloaded {len(complete_data)} Bytes in {time.time()-down_start_time} Seconds')

### FUNCTION TO HANDLE INDIVIDUAL CHUNK DOWNLOADS - LOWER LEVEL. TAKES NODE AND CHUNK NUMBERS AS INPUT.
def downloadFrom(index, src, fname, cstart, cend):
    # buffer
    load = b''
    # connect to remote node through independent thread(2nd degree)
    src_conn = ConnThread(addr=src)
    src_conn.start()
    # for each chunk number from window in RR download the chunk
    for cnumber in range(cstart,cend):
        success = False
        while not success:
            # FUNCTION TO DOWNLOAD SINGLE CHUNK OF A FILE(function of class ConnThread)
            success, chunk_data = src_conn.downloadChunk(fname, cnumber)
            # if failed then try again
            if not success:
                print(f'{fname}#{cnumber} failed retrying...')
                continue
            # if success, add to buffer
            load += chunk_data
            print(f'{fname}#{cnumber} done.')
    # close connection to node and end thread
    src_conn.disconnect()
    return (index, load)

### FUNC TO SCAN FOR NODES IN NETWORK
def updateNodeList():
    global NODE_LIST
    NODE_LIST = []
    ## SCAN BETWEEN 9000 AND 9129 (RANGE 130)
    for i in range(130):
        if 9000 + i == args.port:
            continue
        
        try:
            n = ConnThread(addr=(args.ip,9000+i))
            if (args.ip,9000+i) not in NODE_LIST:
                NODE_LIST.append((args.ip,9000+i))
            n.disconnect()
        except:
            pass
    logger.info(f'{"[NODES DISCOVERED]":<26}{len(NODE_LIST)} Node(s) in Network')

### NOTIFY ALL NODES IN NETWORK FOR LEADER UPDATE
def notifyAll():
    for noti in NODE_LIST:
        try:
            noti = ConnThread(addr = noti)
            noti.updateLeader()
            noti.disconnect()
        except:
            continue

### LEADER ELECTION ALGORITHM
def findDHT():
    updateNodeList()
    logger.info(f'{"[FINDING LEADER]":<26}')
    global LEADER
    global DHT_ADDR
    global dht
    global LEADER_TIME
    global LAST_CHECK
    LAST_CHECK = time.time()

    ## SELF ELECT(IF ALONE)
    if not NODE_LIST:
        if not LEADER:
            LEADER_TIME = time.time()
        LEADER = True
        DHT_ADDR = ADDR
        dht = DHT()
        logger.info(f'{"[WON LEADER]":<26}')
        return True
    ## LOOK UP NETWORK FOR LEADER AND UPDATE DHT
    else:
        for n in NODE_LIST:
            try:
                n = ConnThread(addr = n)
            except:
                continue
            n.start()
            leader_check = n.leaderPing()
            # Update dht if leader found
            if leader_check:
                if LEADER:
                    logger.info(f'{"[LOST LEADER]":<26}{time.time() - LEADER_TIME} Seconds')
                    LEADER = False
                logger.info(f'{"[FOUND LEADER]":<26}{DHT_ADDR}')
                update_dht = n.updateDHT()
                n.disconnect()
                if update_dht == False:
                    return False
                return True
            n.disconnect()

    ## CONTINUE BEING LEADER (IF NO LEADER IN NETWORK) AND UPDATE NETWORK FOR LEADER
    if not LEADER:
        LEADER = True
        LEADER_TIME = time.time()
        dht = DHT()
        DHT_ADDR = ADDR
        notifyAll()
        logger.info(f'{"[WON LEADER]":<26}')
        return True
    DHT_ADDR = ADDR
    logger.info(f'{"[HOLD LEADER]":<26}')
    return True

### BIND AND START LISTENING ONTO PORT
def setupServer():
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    soc.bind(ADDR)
    soc.listen()
    logger.info(f'{"[LISTENING]":<26}On host:{args.ip} and Port:{args.port}')
    while True:
        ## MULTI THREADING CONNECTIONS
        try:
            conn, addr = soc.accept()
            new_node = ConnThread(conn, addr, True)
            new_node.start()
            logger.info(f'{"[ACTIVE CONNECTIONS]":<26}{TOTAL_CONN}')
        
        ## HANDLE ANY OTHER ERRORS
        except Exception as e:
            logger.info(f'{"[ERROR]":<26}Accepting Connection: {e}')

### MAIN APP
if __name__ == "__main__": 
    # IF NOT IN TEST MODE  
    if not args.t:
        logger.info(f'{"[STARTING]":<26}Node {args.port} is starting...')
        if args.t:
            logger.info(f'{"[TEST MODE]":<26}')
        
        ## START LISTENER
        server = threading.Thread(target=setupServer, args=())
        server.start()
        
        ## FIND LEADER
        find_dht = False
        while not find_dht:
            find_dht = findDHT()
        
        ## MAIN APP
        if not LEADER:
            # GET FILE LIST, SELECT FILE AND GET SOURCE LIST FOR SPECIFIC FILE FROM LEADER
            n = ConnThread(addr=DHT_ADDR)
            n.start()
            fl = n.getFileList()
            fl = selectFileFromList(fl)
            
            # SHOW INTENT TO DOWNLOAD SOME FILE
            primary, secondary = n.getFileSources(fl)
            
            # DOWNLOAD OR PASS
            li = int(input(f'DO YOU INTENT TO DOWNLOAD {fl}? 0 - NO , 1 - YES\n'))

            # CASE 1
            if li == 1:
                # DISPLAY LIST
                primary_list = []
                secondary_list = []
                for p in primary:
                    primary_list.append(p[1])
                if secondary:
                    for s in secondary:
                        secondary_list.append(s[1])
                print(f'\nHost Nodes: {primary_list}\nMay Have Nodes: {secondary_list}')
                # USER INTERRACTION TO SELECT SOURCES TO DOWNLOAD FILE FROM
                li = int(input(f'\nSelect where you would like to download from:\n0 - Host Nodes\n1 - Maybe Nodes\n2 - Mix of Both\n'))
                # case 1.1
                if li == 0:
                    downloadHandler(fl, primary)
                # case 1.2
                elif li == 1:
                    downloadHandler(fl, secondary)
                # case 1.3
                else:
                    if secondary:
                        primary = primary + secondary
                    downloadHandler(fl, primary)
                # AFTER DOWNLOAD UPDATE DHT
                n.updateDHT()
                n.disconnect()
            # CASE 2
            else:
                print(f'DONE WITHOUT DOWNLOADING')
                n.disconnect()
        else:
            print('DHT Node')
    
    ## TEST MODE
    else:
        ## START TEST LISTENER
        server = threading.Thread(target=setupServer, args=())
        server.start()
        
        ## TEST LOOP
        while True:
            logger.info(f'{"[STARTING]":<26}Node {args.port} is starting...')
            if args.t:
                logger.info(f'{"[TEST MODE]":<26}')

            logger.info(f'{"[LISTENING]":<26}On host:{args.ip} and Port:{args.port}')

            ## FIND LEADER
            find_dht = False
            while not find_dht:
                find_dht = findDHT()
            
            ## WAIT FOR TEST TO TRIGGER
            while not TEST_START:
                time.sleep(1)
                print(args.port)

            ## START TEST =========

            print('\nTEST STARTED')

            ## REPLICATE BEHAVIOUR OF NORMAL USER MODE WITH PREDEFINED FILE TO DOWNLOAD
            n = ConnThread(addr=DHT_ADDR)
            n.start()
            fl = n.getFileList()
            primary, _ = n.getFileSources(fl[0])
            downloadHandler(fl[0], primary)
            n.updateDHT()
            n.disconnect()

            ## REPORT BANDWIDTH OBSERVATIONS
            logger.info(f'{"[TEST RESULTS]":<26}DOWNLOAD: {TOTAL_DOWN} Bytes; UPLOAD: {TOTAL_UP} Bytes')

            print('\nTEST OVER')

            ## END ========= =========
            TEST_START = False

            ## RESET LOG FILE AFTER 30 SECONDS FOR NEXT TEST (COPY LOGS TO OUT FILE MANUALLY)
            time.sleep(30)
            with open(f'./logs/Node-{args.port}.log', 'w'):
                pass
