"""Automatic-VMS-To-VMS_TEMP-Script

This script automatically detects and transforms newly added key-value
pairs in VMS into relational DB format (VMS_TEMP)

----------Must Read----------
This script does not require any parameter, simply run "python init.py"
will do. If the script is stopped for whatever reason, simply run "python
init.py" will restore the original process, no extra manipulation is required

This script uses Python 2.7.12 and require the following package to be
installed
(1) redislite (2)cx_Oracle (3)zlib (4)logging (5) time (6) datetime (7)pytz

----------Optional You Dont Have To Read This----------
This file contains the following Classes(**) and functions(*):

    ** OracleDB - A class contains operations with Oracle DB
        * connect - connect to Oracle DB
        * close - close the connection to Oracle DB
    ** Zlib - A class using zlib library to (de)compress data
        * compress - compress the given data
        * decompress - decompress the given data
    ** Log - A class using logging library to log to a file
        * writeLog - write content to a log file
    ** RedisDB - A class using redislite to interact with local Redis server
    ** TryCatchException - A class catching exception
        * catchException - try catch an sql statement
    ** Update - A class that determine if syncing is needed and update information of VMS
        * updateTimeTuple - update all distinct timestamp from vms to vms_temp
        * updateTimeDocId - update and compress all corresponding docId to timestamp
        * examine - check if VMS and VMS_TEMP are synced if not run above function and mainFunction
    ** MainProcess - A class that includes all the function that needed to transform VMS to VMS_TEMP
        * getDocIdList - get all the docId as a list correspond to the input timestamp
        * startOrFix - return last timestamp and docId
        * compare - return VMS_TEMP column name based on keys
        * vmsTempPurify - seperate VMS_ANNO_MANAGERS_CHAIN_STR and index with ROW_ID
        * changeToUTC - change input time to UTC timezone time
        * pivot - transpose key-value pair to relational database form
        * mainFunction - main function be called by examine if needed
"""

from redislite import Redis
import cx_Oracle
import zlib
import logging
from logging.handlers import RotatingFileHandler
import time
import datetime
from datetime import timedelta
import pytz

class OracleDB:
    """A class contains operations with Oracle DB"""

    def __init__(self, user, password, server, port, sid):
        """
        Parameters
        __________
        user : str
            username for the Oracle DB
        password : str
            password for the Oracle DB
        server : str
            server ip or hostname 
        port : int
            server port number
        sid: str
            server sid or service name
        """

        self.tns = cx_Oracle.makedsn(server, port, sid)
        self.connection = None
        self.cursor = None
        self.user = user
        self.password = password
    
    def connect(self):
        """connect to Oracle DB"""

        self.connection = cx_Oracle.connect(self.user, self.password, self.tns)
        self.cursor = self.connection.cursor()

    def close(self):
        """close the connection to Oracle DB"""

        self.cursor.close()
        self.connection.close() 

class Zlib:
    """A class using zlib library to (de)compress data"""

    def compress(self, data):
        """compress the given data

        Parameters
        __________
        data : str
            uncompressed data
        """

        compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, +15)  
        compressed_data = compress.compress(data)  
        compressed_data += compress.flush()
        return compressed_data
    
    def decompress(self, compressedData):
        """decompress the given data

        Parameters
        __________
        compressedData : str
            compressed data
        """

        decompress = zlib.decompressobj()  
        decompressed_data = decompress.decompress(compressedData)
        decompressed_data += decompress.flush()
        return decompressed_data

class Log:
    """A class using logging library to log to a file"""

    def __init__(self, logName):
        """
        Parameters
        __________
        logName : str
            log file name
        """

        self.logName = logName
        self.logger = logging.getLogger(self.logName)
        self.logger.setLevel(logging.DEBUG)
        self.handler = RotatingFileHandler(self.logName + '.log', maxBytes=10000, backupCount=1)
        self.logger.addHandler(self.handler)

    def writeLog(self, content):
        """write content to a log file

        Parameters
        __________
        content : str
            log content that want to be written
        """
        
        self.logger.debug(content)

class RedisDB:
    """A class using redislite to interact with local Redis server"""

    def __init__(self, dbPath):
        """
        Parameters
        __________
        dbPath : str
            path of the redis DB
        """

        self.conn = Redis(dbPath)

class TryCatchException:
    """A class catching exception"""

    def __init__(self, db, processLog):
        """
        Parameters
        __________
        db : instance
            instance of class OracleDB
        
        processLog : instance
            instance of class Log
        """

        self.db = db
        self.processLog = processLog
    
    def catchException(self, sql):
        """try and catch exception of a statement

        Parameters
        __________
        sql : str
            an sql statement
        """

        try:
            self.db.cursor.execute(sql)
            self.db.connection.commit()
        except:
            self.processLog.writeLog("ERROR: Something wrong happened when doing "+sql+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            self.db.connection.commit()

class MainProcess:
    """A class that includes all functions that needed to transform VMS to VMS_TEMP"""
    
    def __init__(self, db, redisCurStat, redisTimeTup, redisTimeDocId, comp, processLog, e):
        """
        Parameters
        __________
        db : instance
            instance of class OracleDB
        
        redisCurStat, redisTimeTup, redisTimeDocId : instance
            instance of class RedisDB

        comp : instance
            instance of class Zlib

        processLog : instance
            instance of class Log

        e : instance
            instance of class TryCatchException
        """

        self.db = db
        self.redisCurStat = redisCurStat
        self.redisTimeTup = redisTimeTup
        self.redisTimeDocId = redisTimeDocId
        self.comp = comp
        self.processLog = processLog
        self.e = e

    #create redis database redisCurrentState include(currentTimeKey(redis), docIdIndex(previous, redis), vmsTempRowId(vmstemp), currentRowNum, totalRowNum(VMS))
    
    def getDocIdList(self, timestamp):
        """get all the docId as a list correspond to the input timestamp

        Parameters
        __________
        timestamp : str
            input timestamp and return its corresponding docID
        """

        compressedData = self.redisTimeDocId.conn.get(timestamp)
        decompressedData = self.comp.decompress(compressedData)
        #split the DocIdString into chunks of 24 character
        docIdList = [decompressedData[i:i+24] for i in range(0, len(decompressedData), 24)]
        return sorted(docIdList)
    
    def startOrFix(self):
        """return last timestamp and docId"""

        #if vms_temp is empty then return the earliest timestampkey and its docid list
        #else return the current timestampkey and it's docid list starting from doc_id_index
        maxRowId = self.redisCurStat.conn.get('vmsTempRowId')
        startTimeKey = self.redisCurStat.conn.get('currentTimeKey')
        currentDocIdIndex = self.redisCurStat.conn.get('docIdIndex')
        startTimekey = self.redisTimeTup.conn.get('startTimeKey')
        if maxRowId is None:
            self.processLog.writeLog("Nothing has inserted yet, start inserting from the earliest time")
            #return the doc_id list of the earliest timestamp
            return (0, self.getDocIdList(self.redisTimeTup.conn.get(0)))
        elif currentDocIdIndex == '-1':
            self.processLog.writeLog("Start inserting for a new time")
            return (startTimeKey, self.getDocIdList(self.redisTimeTup.conn.get(startTimeKey)))
        else:
            self.processLog.writeLog("Finish inserting for the same time, but with the next doc_id from last time")
            docIdList = self.getDocIdList(self.redisTimeTup.conn.get(startTimeKey))
            if int(currentDocIdIndex) < len(docIdList)-1:
                return (startTimeKey, docIdList[int(currentDocIdIndex)+1:])
            else:
                #when this docId is the last element in the timestamp
                startTimeKey = int(startTimeKey)+1
                return (startTimeKey, self.getDocIdList(self.redisTimeTup.conn.get(startTimeKey)))

    def compare(self, keyid):
        """return VMS_TEMP column name based on keys

        Parameters
        __________
        keyid : int
            keyid of each key in KEYS
        """

        IdColDic = {
            1000000000000 :"VMS_CLUSTER",
            2000000000000 :"VMS_PARENT",
            3000000000000 :"VMS_POD",
            4000000000000 :"VMS_USAGE",
            4001000000000 :"VMS_USAGE_MEM_PROVISIONED_MB",
            4002000000000 :"VMS_USAGE_MEM_USAGE_MB",
            4003000000000 :"VMS_USAGE_DATASTORE_USAGE_MB",
            4004000000000 :"VMS_USAGE_CPU_PROVISIONED_NUM",
            4005000000000 :"VMS_USAGE_MEM_RESERVED_MB",
            4006000000000 :"VMS_USAGE_CPU_RESERVED_MHZ",
            4007000000000 :"VMS_USAGE_CPU_USAGE_MHZ",
            5000000000000 :"VMS_CREATED_AT",
            6000000000000 :"VMS_STORAGE",
            6123000000000 :"VMS_STORAGE_DATASTORE2",
            6129000000000 :"VMS_STORAGE_DATASTORE1",
            7000000000000 :"VMS_TYPE",
            8000000000000 :"VMS_RESOURCE_POOL",
            9000000000000 :"VMS_UUID",
            11000000000000 :"VMS_MOID",
            12000000000000 :"VMS_BACKING_FOLDER",
            13000000000000 :"VMS_POWERSTATE",
            14000000000000 :"VMS_UPDATED_AT",
            15000000000000 :"VMS_VCENTER",
            16000000000000 :"VMS_BACKING_VM",
            17000000000000 :"VMS_TIMESTAMP",
            18000000000000 :"VMS_ANNO",
            18001000000000 :"VMS_ANNO_CREATETIME",
            18002000000000 :"VMS_ANNO_NIMBUS_TESTBED",
            18003000000000 :"VMS_ANNO_MANAGER",
            18004000000000 :"VMS_ANNO_LEASE_AT",
            18005000000000 :"VMS_ANNO_POD_LOCATION",
            18006000000000 :"VMS_ANNO_LEASE_COUNT",
            18007000000000 :"VMS_ANNO_TESTBED_NODE_NAME",
            18008000000000 :"VMS_ANNO_BRANCH",
            18009000000000 :"VMS_ANNO_POD_NAME",
            18010000000000 :"VMS_ANNO_NIMBUS_CONTEXT",
            18011000000000 :"VMS_ANNO_COST_CENTER",
            18012000000000 :"VMS_ANNO_POD_CONTEXTS",
            18013000000000 :"VMS_ANNO_TESTBED_SPEC_NAME",
            18014000000000 :"VMS_ANNO_LEASE",
            18015000000000 :"VMS_ANNO_DEPLOYER",
            18015001000000 :"VMS_ANNO_DEPLOYER_VCLOUD_BUILD",
            18016000000000 :"VMS_ANNO_STATELESS",
            18017000000000 :"VMS_ANNO_USER",
            18018000000000 :"VMS_ANNO_MANAGERS_CHAIN_STR",
            18019000000000 :"VMS_ANNO_TESTBED_NAME",
            18020000000000 :"VMS_ANNO_SVS_BRANCH",
            18021000000000 :"VMS_ANNO_SVS_PATCH",
            18022000000000 :"VMS_ANNO_SVS_TESTSUITE_OWNERS",
            18023000000000 :"VMS_ANNO_SVS_TESTSUITE",
            18024000000000 :"VMS_ANNO_SVS",
            18025000000000 :"VMS_ANNO_SERVICE",
            18026000000000 :"VMS_ANNO_SVS_HOST",
            18027000000000 :"VMS_ANNO_CAT_BRANCH",
            18028000000000 :"VMS_ANNO_CAT_VIEWTYPE_ID",
            18029000000000 :"VMS_ANNO_CAT_TESTRUN",
            18030000000000 :"VMS_ANNO_CAT_SITE",
            18031000000000 :"VMS_ANNO_CAT_SLATYPE",
            18032000000000 :"VMS_ANNO_CAT_WORKLOAD",
            18033000000000 :"VMS_ANNO_CAT_SITE_ID",
            18034000000000 :"VMS_ANNO_CAT_VIEWTYPE",
            18035000000000 :"VMS_ANNO_CAT_AREA_ID",
            18036000000000 :"VMS_ANNO_CAT_SLATYPE_ID",
            18037000000000 :"VMS_ANNO_CAT_AREA",
            18038000000000 :"VMS_ANNO_KEEPVMSONFAILURE",
            18039000000000 :"VMS_ANNO_VCVA_BUILD",
            18040000000000 :"VMS_ANNO_TEMPLATE_TYPE",
            18041000000000 :"VMS_ANNO_TEMPLATE",
            18042000000000 :"VMS_ANNO_TEMPLATE_NAME",
            18043000000000 :"VMS_ANNO_NSX_NETWORK",
            18044000000000 :"VMS_ANNO_GATEWAY_POD",
            18045000000000 :"VMS_ANNO_GATEWAY",
            18046000000000 :"VMS_ANNO_BUILD",
            18047000000000 :"VMS_ANNO_VMTREE",
            18048000000000 :"VMS_ANNO_DYNAMICWORKER",
            18049000000000 :"VMS_ANNO_TEST_VPX",
            18050000000000 :"VMS_ANNO_TAGS",
            18051000000000 :"VMS_ANNO_IP",
            18052000000000 :"VMS_ANNO_VSM_BUILD",
            18053000000000 :"VMS_ANNO_LEASE_AB_REAPER",
            18054000000000 :"VMS_ANNO_LEASE_AB_STORM_REAPER",
            18055000000000 :"VMS_ANNO_LEASED_AT",
            18056000000000 :"VMS_ANNO_XVC_VMOTION",
            18057000000000 :"VMS_ANNO_LDAP_AB_STORM_REAPER",
            18058000000000 :"VMS_ANNO_MTREE",
            18059000000000 :"VMS_ANNO_MANAGER_UID",
            18060000000000 :"VMS_ANNO_NAG_USER",
            18061000000000 :"VMS_ANNO_NOTES",
            18062000000000 :"VMS_ANNO_DVM",
            18063000000000 :"VMS_ANNO_PAGE",
            18064000000000 :"VMS_ANNO_MODIFIER",
            18065000000000 :"VMS_ANNO_UPDATES",
            18066000000000 :"VMS_ANNO_CAT_SIDE_ID",
            18067000000000 :"VMS_ANNO_FOR_LABEL",
            18068000000000 :"VMS_ANNO_TEST",
            18069000000000 :"VMS_ANNO__USER",
            18070000000000 :"VMS_ANNO_NAME",
            18071000000000 :"VMS_ANNO_ALPS_TESTBED",
            18072000000000 :"VMS_ANNO_ALPS_LAUNCHER",
            18073000000000 :"VMS_ANNO_ALPS_CRS_JOB",
            18074000000000 :"VMS_ANNO_ALPS_HOST1_NAME",
            18075000000000 :"VMS_ANNO_ALPS_VC1_BUILD",
            18076000000000 :"VMS_ANNO_ALPS_VC1_NAME",
            18077000000000 :"VMS_ANNO_ALPS_HOST1_BUILD",
            18078000000000 :"VMS_ANNO_ALPS_HOST1_BRANCH",
            18079000000000 :"VMS_ANNO_ALPS_VC1_BRANCH",
            18080000000000 :"VMS_ANNO_JENKINS_URL",
            18081000000000 :"VMS_ANNO_BUTLER",
            18082000000000 :"VMS_ANNO_ALPS_HOST3_BRANCH",
            18083000000000 :"VMS_ANNO_ALPS_HOST2_BRANCH",
            18084000000000 :"VMS_ANNO_DESCRIPTION",
            18085000000000 :"VMS_ANNO_EXECUTOR",
            18086000000000 :"VMS_ANNO_HOSTNAME",
            18087000000000 :"VMS_ANNO_SERVICENAME",
            18088000000000 :"VMS_ANNO_SERVICE_NAME",
            18089000000000 :"VMS_ANNO_USER_",
            18090000000000 :"VMS_ANNO_TESTBED_NAME",
            18091000000000 :"VMS_ANNO_PRODUCT",
            18092000000000 :"VMS_ANNO_BS_URL",
            18093000000000 :"VMS_ANNO_BS_DESCRIPTION",
            18094000000000 :"VMS_ANNO_BS_NAME",
            18095000000000 :"VMS_ANNO_BS_EXECUTOR",
            18096000000000 :"VMS_ANNO_BS_TYPE",
            18097000000000 :"VMS_ANNO_BS_TEMPLATE",
            18098000000000 :"VMS_ANNO_FRAMEWORK",
            18099000000000 :"VMS_ANNO_CUSTOM_NOTE",
            18100000000000 :"VMS_ANNO_PASSWORD",
            18101000000000 :"VMS_ANNO_USERNAME",
            18102000000000 :"VMS_ANNO_TESTBEDID",
            18103000000000 :"VMS_ANNO_CAT",
            18104000000000 :"VMS_ANNO_NO_VSAN",
            18108000000000 :"VMS_ANNO_TEST_INSTANCE_ID",
            18109000000000 :"VMS_ANNO_TEST_FUNCTION_AREA",
            18110000000000 :"VMS_ANNO_TEST_SET_NAME",
            18111000000000 :"VMS_ANNO_TEST_SET_ID",
            18112000000000 :"VMS_ANNO_TEST_INSTANCE_NAME",
            18113000000000 :"VMS_ANNO_TEST_COMPO",
            18114000000000 :"VMS_ANNO_QUOTA_PROJECT",
            18115000000000 :"VMS_ANNO_TEST_SET",
            18116000000000 :"VMS_ANNO_TEST_TYPE",
            18117000000000 :"VMS_ANNO_TEST_COMPO_VMCRYPT",
            18118000000000 :"VMS_ANNO_TEST_COMPONVM_VMLC",
            18119000000000 :"VMS_ANNO_JENKINS_ID",
            18122000000000 :"VMS_ANNO_TEST_SERVICE",
            18123000000000 :"VMS_ANNO_PRODUCTION",
            18124000000000 :"VMS_ANNO_QUOTA_RC",
            18125000000000 :"VMS_ANNO_QUOTA_PROJECT",
            18126000000000 :"VMS_ANNO_PRODUCT",
            18128000000000 :"VMS_ANNO_AREA",
            18129000000000 :"VMS_ANNO_QUOTA_PROJECT",
            18130000000000 :"VMS_ANNO_QUOTA_PROJECT",
            18131000000000 :"VMS_ANNO_CAT_LAUNCHTYPE",
            18132000000000 :"VMS_ANNO_NO_NAG",
            18133000000000 :"VMS_ANNO_KVM_HOSTWFS",
            18134000000000 :"VMS_ANNO_UUID",
            18135000000000 :"VMS_ANNO_INFLUXDB_USERNAME",
            18136000000000 :"VMS_ANNO_INFLUXDB_PASSWORD",
            18137000000000 :"VMS_ANNO_CONCOURSE_WORKER_NAME",
            18138000000000 :"VMS_ANNO_INFLUXDB_DATABASE",
            18139000000000 :"VMS_ANNO_INFLUXDB_URL",
            18140000000000 :"VMS_ANNO_TEAM",
            18141000000000 :"VMS_ANNO_TAG",
            18142000000000 :"VMS_ANNO_DUMMY",
            18143000000000 :"VMS_ANNO_DCPN",
            18144000000000 :"VMS_ANNO_DISK",
            18145000000000 :"VMS_ANNO_ESX",
            18146000000000 :"VMS_ANNO_ESX",
            18147000000000 :"VMS_ANNO_LANG",
            18148000000000 :"VMS_ANNO_ENABLECUSTOMLOCATION",
            18149000000000 :"VMS_ANNO_COMMENT",
            18150000000000 :"VMS_ANNO_VIVI_TEST",
            18151000000000 :"VMS_ANNO_NIMBUS_TESTBED",
            18152000000000 :"VMS_ANNO_COST_CENTER",
            18153000000000 :"VMS_ANNO_MANAGERS_CHAIN_STR",
            18154000000000 :"VMS_ANNO_MANAGER",
            18155000000000 :"VMS_ANNO_USER",
            18156000000000 :"VMS_ANNO_4176_69PRODUCT",
            18157000000000 :"VMS_ANNO_RUNTYPE",
            18158000000000 :"VMS_ANNO_TESTSUITE",
            18159000000000 :"VMS_ANNO_PROJECTPATH",
            18160000000000 :"VMS_ANNO_CHANGELIST",
            18161000000000 :"VMS_ANNO_DESTRESOURCEPOOL",
            18162000000000 :"VMS_ANNO_VMHOMEPF",
            18163000000000 :"VMS_ANNO_VDISKSIZEMAP",
            18163001000000 :"VMS_ANNO_VDISKSIZEMAP_HD2",
            18163002000000 :"VMS_ANNO_VDISKSIZEMAP_HD1",
            18164000000000 :"VMS_ANNO_NUMVNIC",
            18165000000000 :"VMS_ANNO_VNIC",
            18166000000000 :"VMS_ANNO_NUMVDISK",
            18167000000000 :"VMS_ANNO_FSTPM",
            18167001000000 :"VMS_ANNO_FSTPM_PF1_ST_SHARE_1",
            18167002000000 :"VMS_ANNO_FSTPM_PF2_ST_SHARE_2",
            18168000000000 :"VMS_ANNO_DESTHOSTNAME",
            18169000000000 :"VMS_ANNO_VDISKTOPFMAP",
            18169001000000 :"VMS_ANNO_VDISKTOPFMAP_HD2",
            18169002000000 :"VMS_ANNO_VDISKTOPFMAP_HD1",
            18170000000000 :"VMS_ANNO_MEMORYINMB",
            18171000000000 :"VMS_ANNO_MRLTM",
            18172000000000 :"VMS_ANNO_NUMCPU",
            18173000000000 :"VMS_ANNO_DESTDATASTORE",
            18174000000000 :"VMS_ANNO_SVS_LOCATION",
            18175000000000 :"VMS_ANNO_NIMBUS_WORKLOAD_PRIO",
            19000000000000 :"VMS_VCENTER_UUID",
            20000000000000 :"VMS_LEASE_ENDS_AT",
            21000000000000 :"VMS_RAW_ANNO",
            22000000000000 :"VMS_NAME",
            23000000000000 :"VMS_INSTANCEUUID",
            24000000000000 :"VMS_GUEST",
            24001000000000 :"VMS_GUEST_HOSTNAME",
            24002000000000 :"VMS_GUEST_IPADDRESS",
            24003000000000 :"VMS_GUEST_NICS",
            25000000000000 :"VMS_USER",
            26000000000000 :"VMS_HOST_NAME",
            27000000000000 :"VMS_DESTROYED_AT",
            28000000000000 :"VMS_MEMBEROFVAPP"
        }
        return IdColDic.get(keyid)

    def vmsTempPurify(self, lastDocId, manager_str, col_val_dic):
        """seperate VMS_ANNO_MANAGERS_CHAIN_STR and index with ROW_ID

        Parameters
        __________
        lastDocId : int
            index of last docId in docIdList
        """

        #(1) do managerStr and indexing (2) add 1 to vmsTempRowId (3) update docIdIndex + 1
        #sql = "Select VMS_ANNO_MANAGERS_CHAIN_STR from vms_temp where doc_id = '%s'" % lastDocId
        #self.e.catchException(sql)
        #rows = self.db.cursor.fetchall()
        #get the vmsTempRowId from redis, add one to it and use it
        rowId = self.redisCurStat.conn.get('vmsTempRowId')
        if rowId is None:
            rowId = -1
        rowId = int(rowId) + 1
        #add one to vmsTempRowId
        self.redisCurStat.conn.set('vmsTempRowId',rowId)
        #Indexing vms_temp
        #sql = "UPDATE vms_temp SET row_id='%s' WHERE doc_id = '%s';"%(rowId, lastDocId)
        #sqlStr = sqlStr + sql
        col_val_dic["row_id"] = rowId
        if manager_str is not None:
            out = [x[3:] for x in manager_str.split('/')]
            #update and divide manager string
            for i in range(len(out)):
                #sql = "UPDATE vms_temp SET VMS_ANNO_MANAGERS_CHAIN_STR_%s = '%s' WHERE doc_id = '%s';"%(i, out[i], lastDocId)
                #sqlStr = sqlStr + sql
                col_val_dic["VMS_ANNO_MANAGERS_CHAIN_STR_"+str(i)] = out[i]
        docIdIndex = self.redisCurStat.conn.get('docIdIndex')
        docIdIndex = int(docIdIndex) + 1
        self.redisCurStat.conn.set('docIdIndex', docIdIndex)
        return col_val_dic
    
    def changeToUTC(self, originalTime):
        """change input time to UTC timezone time

        Parameters
        __________
        originalTime : str
            original time with random timezone
        """

        originalTimeList = [time for time in originalTime.split(' ')]
        timeZoneHour = int(originalTimeList[2][0:3])
        timeZoneMin = int(originalTimeList[2][3:])
        utc_offset = timedelta(hours=timeZoneHour, minutes=timeZoneMin)
        now = datetime.datetime.now(pytz.utc)
        currentTimeZone = [tz.zone for tz in map(pytz.timezone, pytz.all_timezones_set) if now.astimezone(tz).utcoffset() == utc_offset][0]
        currentTime = originalTimeList[0] + ' ' + originalTimeList[1]
        local = pytz.timezone (currentTimeZone)
        naive = datetime.datetime.strptime (currentTime, "%Y-%m-%d %H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc).strftime ("%Y-%m-%d %H:%M:%S")
        return utc_dt

    def pivot(self, docIdList):
        """transpose key-value pair to relational database form

        Parameters
        __________
        docIdList : list
            list of docId
        """

        format_strings = ','.join(["(1,'%s')"] * len(docIdList))
        sql = "select * from vms where (1, doc_id) in (%s) order by doc_id" % format_strings % tuple(docIdList)
        self.e.catchException(sql)
        origin = self.db.cursor.fetchall()
        lastDocId = origin[0][0]
        #see if each rows's doc_id exists in vms_temp
        sql = "select * from vms_temp where doc_id = '%s' and rownum <= 1" % lastDocId
        self.e.catchException(sql)
        docIdExists = self.db.cursor.fetchall()
        manager_str = ""
        col_val_dic = {}
        #read each row from vms
        for row in origin:
            #if docid change do below steps to the lastDocId (1) do managerStr and indexing (2) add 1 to vmsTempRowId (3) update docIdIndex=lastDocId
            if lastDocId != row[0]:
                col_val_dic = self.vmsTempPurify(lastDocId, manager_str, col_val_dic)
                keys = [k for k in col_val_dic]
                val = [col_val_dic[k] for k in col_val_dic]
                insert_val = ", ".join(["%s= '%s'"%kv for kv in zip(keys, val)])
                sql = "UPDATE VMS_TEMP SET " + insert_val
                sql = sql + " WHERE doc_id = '%s'" % lastDocId
                self.e.catchException(sql)
                col_val_dic = {}
                #see if each rows's doc_id exists in vms_temp
                docIdExists = False
            #get the column_name from vms_temp
            vmsTempCol = self.compare(row[1])
            if vmsTempCol is None:
                continue
            #if col is timestamp then change its format to utc
            if vmsTempCol == 'VMS_TIMESTAMP':
                row = list(row)
                row[3] = self.changeToUTC(row[3])
                row = tuple(row)
            elif vmsTempCol == 'VMS_ANNO_MANAGERS_CHAIN_STR':
                manager_str = row[3]
            #yes - then insert into the original row with the same doc_id
            if docIdExists:
                #catch exception and write log if error
                col_val_dic[vmsTempCol] = row[3]
            #no - create a new row with the new doc_id
            else:
                #catch exception and write log if error
                sql = "insert into vms_temp (doc_id, %s) values ('%s', '%s')"%(vmsTempCol, row[0], row[3])
                self.e.catchException(sql)
                docIdExists = 1
            #plus one to currentRowNum
            rowId = self.redisCurStat.conn.get('currentRowNum')
            if rowId is None:
                rowId = 0
            rowId = int(rowId) + 1
            self.redisCurStat.conn.set('currentRowNum',rowId)
            lastDocId = row[0]
        col_val_dic = self.vmsTempPurify(lastDocId, manager_str, col_val_dic)
        keys = tuple([k for k in col_val_dic])
        val = tuple([col_val_dic[k] for k in col_val_dic])
        insert_val = ", ".join(["%s= '%s'"%kv for kv in zip(keys, val)])
        sql = "UPDATE VMS_TEMP SET " + insert_val
        sql = sql + " WHERE doc_id = '%s'" % lastDocId
        self.e.catchException(sql)

    def mainFunction(self):
        """main function be called by examine if needed"""

        self.processLog.writeLog("Initializing, Getting the start timestamp and docId list......")
        #use the redis timeTuple and startTimeKey to form the outter loop
        (startTimeKey, docIdList) = self.startOrFix()
        startTimeKey = int(startTimeKey)
        lastTimeKey = self.redisCurStat.conn.get('currentTimeKey')
        if lastTimeKey is not None:
            lastTimeKey = int(lastTimeKey)
        for timeKey in range(startTimeKey, self.redisTimeTup.conn.dbsize()):
            if timeKey != lastTimeKey:
                #set or reset docIdIndex to -1 and set it to redis
                self.redisCurStat.conn.set('docIdIndex', -1)
            #update currentTimeKey = timeKey
            self.redisCurStat.conn.set('currentTimeKey', timeKey)
            if timeKey == startTimeKey:
                self.processLog.writeLog("Finishing the process of Inserting rows for last time key %s" % timeKey)
                #if timeKey = startTimeKey then execute the rest part of docid list
                self.pivot(docIdList)
            else:
                self.processLog.writeLog("Start Inserting rows for the time key %s" %timeKey)
                #else execute the rest timekey and its full docid list
                currentTimeStamp = self.redisTimeTup.conn.get(timeKey)
                docIdList = self.getDocIdList(currentTimeStamp)
                self.pivot(docIdList)
            lastTimeKey = timeKey

class Update:
    """A class that determine if syncing is needed and update information of VMS"""

    def __init__(self, db, redisCurStat, redisTimeTup, redisTimeDocId, comp, processLog, e):
        """
        Parameters
        __________
        db : instance
            instance of class OracleDB
        
        redisCurStat, redisTimeTup, redisTimeDocId : instance
            instance of class RedisDB

        comp : instance
            instance of class Zlib

        processLog : instance
            instance of class Log

        e : instance
            instance of class TryCatchException
        """

        self.db = db
        self.redisCurStat = redisCurStat
        self.redisTimeTup = redisTimeTup
        self.redisTimeDocId = redisTimeDocId
        self.comp = comp
        self.processLog = processLog
        self.e = e

    def updateTimeTuple(self):
        """update all distinct timestamp from vms to vms_temp"""

        self.processLog.writeLog("Start selecting all timestamp from VMS...")
        #TODO: unset the rownum  below
        sql = "SELECT DISTINCT(value) FROM VMS WHERE key = '17000000000000' and rownum < 50000 ORDER BY value"
        self.e.catchException(sql)
        timeTuple = self.db.cursor.fetchall()
        timeTupleLen = len(timeTuple)
        self.processLog.writeLog("Start getting number of current timestamps from Redis...")
        #get the number of keys(distinct time) in the redis database
        currentTimeNum = self.redisTimeTup.conn.dbsize()
        #get the offset of total time num and current time num
        timeNumOffset = timeTupleLen - currentTimeNum
        if timeNumOffset != 0:
            self.processLog.writeLog("timestamp Num mismatch start executing the update process...")
            #update the new timeTuple and there corresponding time docid using the updateTimeDocId function
            #get the extra time and form them as a list
            #get the start storing index in redis for the extra time
            startIndex = currentTimeNum
            extraTimeList = []
            for i in range(startIndex, len(timeTuple)):
                #TODO: uncomment below line
                #self.redisTimeTup.conn.set(i, timeTuple[i][0])
                extraTimeList.append(timeTuple[i][0])
            self.processLog.writeLog("Timestamp update success...")
            self.processLog.writeLog("Start updating the corresponding DocID...")
            self.updateTimeDocId(extraTimeList)
    
    def updateTimeDocId(self, extraTimeList):
        """update and compress all corresponding docId to timestamp

        Parameters
        __________
        extraTimeList : list
            list of newly added timestamps in VMS
        """

        #TODO: unset below rownum
        sql = "select DISTINCT(doc_id), value from vms where key = '17000000000000' and rownum < 80000"
        self.e.catchException(sql)
        result = self.db.cursor.fetchall()
        #store the (doc_id, vlaue) pair into redis database with value as key and doc_id compressed string as value
        i = 0
        for (docId, time) in result:
            if time in extraTimeList:
                #if redisTimeDocId is empty then insert all value of redisTimeTuple as keys and empty string as value
                if self.redisTimeDocId.conn.dbsize() == 0:
                    for timeKey in self.redisTimeTup.conn.keys():
                        self.redisTimeDocId.conn.set(self.redisTimeTup.conn.get(timeKey), "")
                #match time with the keys in redisTimeDocId, if match then value = value + docId
                compressedDocIdList = self.redisTimeDocId.conn.get(time)
                if compressedDocIdList != None:
                    decompressedDocIdList = self.comp.decompress(compressedDocIdList)
                    decompressedDocIdList = decompressedDocIdList + docId
                    #compress the string
                    compressedDocIdList = comp.compress(decompressedDocIdList)
                    #store the string into the redis db with the key as the time
                    #TODO: uncomment below line
                    #self.redisTimeDocId.conn.set(time, compressedDocIdList)
                else:
                    compressedDocIdList = docId
                    #compress the string
                    compressedDocIdList = self.comp.compress(compressedDocIdList)
                    #store the string into the redis db with the key as the time
                    #TODO: uncomment below line
                    #self.redisTimeDocId.conn.set(time, compressedDocIdList)
            i = i + 1
        self.processLog.writeLog("Total %s timestamp's docId update successful..." % i)

    #Check if need to sync VMS and VMS_TEMP
    def examine(self):
        """check if VMS and VMS_TEMP are synced if not run function mainFunction"""

        self.processLog.writeLog("Start Counting the total row number in VMS and Updating redis current state database...")
        #TODO: unset the rownum below
        sql = "SELECT COUNT(*) FROM VMS where rownum < 500"
        self.e.catchException(sql)
        totalRowNum = self.db.cursor.fetchall()[0][0]
        #update the totalRowNum in redis
        #TODO: uncomment below line
        #self.redisCurStat.conn.set('totalRowNum', totalRowNum)
        self.processLog.writeLog("Updating total row number in VMS Complete, total row number: %s..." % totalRowNum)
        self.processLog.writeLog("Get row number already fetched from VMS from redis...")
        #get currentRowNum in Redis
        if self.redisCurStat.conn.get('currentRowNum') is None:
            currentRowNum = 0
        else:
            currentRowNum = int(self.redisCurStat.conn.get('currentRowNum'))
        self.processLog.writeLog("Get row number already fetched from VMS Complete, current fetched row number: %s..." % currentRowNum)
        #if row number do not match then start executing main.py
        if totalRowNum - currentRowNum != 0:
            self.processLog.writeLog("Total row number and current fetch row number mismatch, Start updating total timestamp and its correspond docID...")
            self.updateTimeTuple()
            self.processLog.writeLog("Update process success...")
            self.processLog.writeLog("Start Executing main function...")
            #Execute mainFunction in MainProcess
            mainP = MainProcess(db, redisCurStat, redisTimeTup, redisTimeDocId, comp, processLog, e)
            mainP.mainFunction()
        else:
            self.processLog.writeLog("VMS_TEMP is already synced")

if __name__ == "__main__":
    #TODO set a time insterval to execute the script
    db = OracleDB("ce_nim_dwh", "P2Nimth1R$3", "PRDAP-DWDB.ENG.VMWARE.COM", 1521, "PRDAPD")
    db.connect()
    db.cursor.arraysize = 10000
    redisCurStat = RedisDB('/tmp/redisCurrentState.db')
    redisTimeTup = RedisDB('/tmp/redisTimeTuple.db')
    redisTimeDocId = RedisDB('/tmp/redisTimeDocId.db')
    comp = Zlib()
    processLog = Log('processLog')
    e = TryCatchException(db, processLog)
    
    update = Update(db, redisCurStat, redisTimeTup, redisTimeDocId, comp, processLog, e)
    update.examine()
    db.close()
    
