#!/usr/local/bin/python

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf
from pyspark.sql import Row, HiveContext, SQLContext
from impala.dbapi import connect
import datetime
import sys
import re
import time
import os
import commands
import ConfigParser
import pytz
from pyspark.sql.types import TimestampType, IntegerType, StringType, DoubleType, DecimalType, LongType, ShortType, BooleanType

# Global Variable
BHV = {}

# Check Arguments
def checkArguments():
    if len(sys.argv) != 3:
        print "Need config file & Environment to proceed. Exiting..."
        sys.exit(-1)
    else:
        print "Arguments defined. Process starting..."
        defineGlobals()

# Read config File
def readConfig(configFile):
    confParser = ConfigParser.RawConfigParser()
    confParser.read(configFile)
    return confParser

# Get HDFS File
def getHDFSFile(fileName, checker):
    localFile = "/tmp/" + os.path.basename(fileName)
    if os.path.isfile(localFile):
        print localFile + " already exists. Ignore HDFS get"
    else:
        try:
            rc = commands.getstatusoutput("hdfs dfs -get " + fileName + " /tmp/")
        except Exception as e:
            print "Printing Exception1 below:"
            print e
            pass
    if checker and not os.path.isfile(localFile):
        print "HDFS Get failed to download config file. Exiting..."
        sys.exit(-1)
    return localFile

# Initialize Global Variables
def defineGlobals():
    hdfsConfig = sys.argv[1]
    environment = sys.argv[2]
    BHV['configFile'] = getHDFSFile(hdfsConfig, 1)
    configParser = readConfig(BHV['configFile'])

    # Database Info
    BHV['impalaSvr'] = configParser.get(environment + 'DatabaseInfo', 'impalaSvr')
    BHV['impalaPort'] = configParser.get(environment + 'DatabaseInfo', 'impalaPort')
    BHV['impalaKerberos'] = configParser.get(environment + 'DatabaseInfo', 'impalaKerberos')
    BHV['SSL'] = configParser.get(environment + 'DatabaseInfo', 'SSL')
    BHV['authenticate'] = configParser.get(environment + 'DatabaseInfo', 'authenticate')
    BHV['certificate'] = configParser.get(environment + 'DatabaseInfo', 'certificate')
    BHV['database'] = configParser.get(environment + 'DatabaseInfo', 'database')
    BHV['keytab'] = configParser.get(environment + 'DatabaseInfo', 'keytab')
    BHV['user'] = configParser.get(environment + 'DatabaseInfo', 'user')

    # Table Information
    BHV['delimiter'] = configParser.get('GlobalVariables', 'delimiter')
    BHV['partitonColumn'] = configParser.get('GlobalVariables', 'partitonColumn')
    BHV['preload'] = configParser.get('GlobalVariables', 'preload')
    BHV['postload'] = configParser.get('GlobalVariables', 'postload')
    BHV['factLoad'] = configParser.get('GlobalVariables', 'factLoad')
    BHV['uniqKey'] = configParser.get('GlobalVariables', 'uniqKey').split(BHV['delimiter'])
    BHV['convReqTypes'] = [['INT', IntegerType()], ['DOUBLE', DoubleType()], ['DECIMAL(38,3)', DecimalType(38, 3)], ['TIMESTAMP', TimestampType()]]
    BHV['apnDir'] = configParser.get('GlobalVariables', 'apnDir')

    # Table Schema
    BHV['cstbhv_call_fact'] = configParser.get('TableSchema', 'cstbhv_call_fact')
    BHV['cstbhv_call_fact_transfer'] = configParser.get('TableSchema', 'cstbhv_call_fact_transfer')
    BHV['cstbhv_data_request'] = configParser.get('TableSchema', 'cstbhv_data_request')
    BHV['cstbhv_segment'] = configParser.get('TableSchema', 'cstbhv_segment')
    BHV['cstbhv_prompt_collect_fact'] = configParser.get('TableSchema', 'cstbhv_prompt_collect_fact')


# Get Current timestamp
def getCurrentTimestamp():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

# Convert epoch to timestamp
# Input - Epoch Time
# Output - Timestamp
def epochToTimestamp(epochTime):
    fmt='%Y-%m-%d %H:%M:%S'
    utcTime = time.strftime(fmt, time.localtime(int(epochTime)))
    date=datetime.datetime.strptime(utcTime, fmt)
    dateUTC = pytz.utc.localize(date,is_dst=None)
    dateEastern=dateUTC.astimezone(pytz.timezone('US/Eastern'))
    return dateEastern.strftime(fmt)

# Convert epoch to YYYYMMDD Format
# Input - Epoch Time
# Output - Date - YYYYMMDD
def epochToYYYYMMDD(epochTime):
    return time.strftime('%Y%m%d', time.localtime(int(epochTime)))

# Convert epoch to HH Format
# Input - Epoch Time
# Output - Hour - HH
def epochToHH(epochTime):
    return time.strftime('%H', time.localtime(int(epochTime)))

# Get 7 days old epoch time
def getStartEpoch():
    currentEpoch = time.strftime('%s', time.localtime())
    oldEpoch = int(currentEpoch) - (7 * 24 * 60 * 60)
    return oldEpoch - ( oldEpoch % (24 * 60 * 60))

# Refresh Impala after load
# Input - Tablename, Partition names list, Partition location list
# Output - None
def refreshImpala(tblName, partition, partitionDir, refreshType):
    partitionName =  BHV['partitonColumn'] + '=' + str(partition)
    try:
        #BHV['keytabLocalFile'] = getHDFSFile(BHV['keytab'], 0, BHV['tableName'])
        BHV['keytabLocalFile'] = getHDFSFile(BHV['keytab'], 0)
        rc = commands.getstatusoutput("kinit -kt " + BHV['keytabLocalFile'] + " " + BHV['user'])
        conn = connect(host=BHV['impalaSvr'], port=BHV['impalaPort'], auth_mechanism=BHV['authenticate'],kerberos_service_name=BHV['impalaKerberos'],use_ssl=BHV['SSL'])
        iCursor = conn.cursor()
        if refreshType == 'STAGE':
            iCursor.execute("ALTER TABLE " + tblName + " ADD IF NOT EXISTS PARTITION (" + partitionName + ") LOCATION '" +  partitionDir + "'")
        iCursor.execute("ALTER TABLE " + tblName + " PARTITION (" + partitionName + ") SET LOCATION '" +  partitionDir + "'")
        if refreshType == 'FACT':
           print "inside fact -->"
           iCursor.execute("REFRESH " + tblName + "")
           iCursor.execute("COMPUTE INCREMENTAL STATS " + tblName + " PARTITION (" + partitionName + ") ")
        #iCursor.execute("REFRESH " + tblName + "")
        #iCursor.execute("ALTER TABLE " + tblName + " ADD IF NOT EXISTS PARTITION (" + partitionName + ") location '" +  partitionDir + "'")
        #iCursor.execute("COMPUTE INCREMENTAL STATS " + tblName + " PARTITION (" + partitionName + ") ")
    except Exception as e:
        print "Printing Exception2 below:"
        print e
        pass
    return 1


# Parse every record type
# Input - Pipe delimited record, Location of epoch time fields, Total fields in the records
# Output - Formatted record list
def parseInputRecord(line, indexes,totalFields):
    result = []
    oldapn =''
    xmlEnd=['<END>']
    tbl=line.split('|')[0]
    if tbl in ('Data Request'):
       for tag in xmlEnd:
           pattern = re.compile(r"\<START\>.*?\<END\>")
           line_parts = pattern.sub(lambda match: match.group(0).replace("|",","),line)
    else:
       line_parts = line
    line_parts = line_parts.split('|')
    tbl=line.split('|')[0]
    for index, part in enumerate(line_parts):
        try:
            if index in indexes: result.append(epochToTimestamp(float(part)))
            if index <= 16:
               if index == 4 and 'apnDict' in BHV:
                  if part[3:] in BHV['apnDict'] and part[:3] <> BHV['apnDict'][part[3:]]:
                     oldapn = part[:3]
                     part = BHV['apnDict'][part[3:]] + part[3:]
                  else:
                     oldapn =''
               if tbl in ('Segment') and index == 5:
                  if line_parts[index].upper() == 'CUSTOMEVENT':
                      if line_parts[index+1].count(':') == 1:
                         segment_group = line_parts[index]
                      else:
                         segment_group = line_parts[index+1].split(':')[0]
                  elif line_parts[index].upper() == 'CUSTOM':
                      if line_parts[index+1].count(':') == 1:
                        segment_group = line_parts[index]
                      else:
                        segment_group = line_parts[index+1].split(':')[0]
                  else:
                     segment_group = line_parts[index]
                  part=''.join(segment_group)
               if tbl in ('Segment') and index == 6:
                  if line_parts[index-1].upper() == 'CUSTOMEVENT':
                     if line_parts[index].count(':') == 1:
                        segment=line_parts[index]
                     else:
                         segment=':'.join(line_parts[index].split(':')[1:])
                         segment=segment[:-1] if segment.endswith(':') else segment
                  elif line_parts[index-1].upper() == 'CUSTOM':
                     if line_parts[index].count(':') == 1:
                        segment=line_parts[index]
                     else:
                         segment=':'.join(line_parts[index].split(':')[1:])
                         segment=segment[:-1] if segment.endswith(':') else segment
                  else:
                     segment=line_parts[index]
                  part=''.join(segment)
               result.append(part)
        except:
            result.append('')

    result = result[:totalFields-1]
    if tbl in ('Inbound','Outbound','inbound','outbound'):
       try:
          resultevnt = round(float(line_parts[8]) -float(line_parts[7]))
       except ValueError:
          resultevnt = 0
       result.append(resultevnt)
       result.append(oldapn)
    elif tbl in ('Bridge Xfer','Network Xfer','network xfer','bridge xfer'):
       try:
          resultevnt = round(float(line_parts[9]) -float(line_parts[8]))
       except ValueError:
          resultevnt = 0
       result.append(resultevnt)
       result.append(oldapn)
    elif tbl in ('Prompt-and-Collect'):
       try:
          resultevnt = round(float(line_parts[7]) -float(line_parts[6]))
       except ValueError:
          resultevnt = 0
       result.append(resultevnt)
       result.append(oldapn)
    result.append(getCurrentTimestamp())
    result = result[:totalFields]
    result.append(epochToYYYYMMDD(line_parts[1]))
    status = 'GOOD' if len(result) == totalFields + 1 else 'BAD'
    return (status, result)


# Get field names from Schemas
# Input - Schema name, optional type
# Output - List of fields
def getFields(fields, dType):
    result = []
    fieldsList = fields.split(BHV['delimiter'])
    for field in fieldsList:
        if dType == None:
            result.append(field.split(' ')[0])
        elif dType.upper() == field.split(' ')[1].upper():
            result.append(field.split(' ')[0])
    return result

# Convert data within dataframe
# Input - dataframe, list of columns to be converted, Type to be converted
# Output - dataframe after conversion
def castStrFields(dataFrame, columns, castType):
    for column in columns:
        dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(castType))
    return dataFrame

# Load data to the table
# Input - Table Layout, Table Name, RDD
# Output - None
def processTable(tblLayout, tableName, rdd):
    if rdd.isEmpty():
        print "EMPTY RDD. Not doing anything!!"
        return
    else:
        dataFrame = sqlContext.createDataFrame(rdd, getFields(tblLayout, None))
        dataFrame.show()

    for types in BHV['convReqTypes']:
        cols = getFields(tblLayout, types[0])
        if len(cols) > 0:
            dataFrame = castStrFields(dataFrame, cols, types[1])

    uniqPartitions = dataFrame.select( BHV['partitonColumn']).distinct().flatMap(lambda x: x).collect()
    print "uniqPartitions",uniqPartitions
    dataFrame.show()
    print "partition column", BHV['partitonColumn']
    allPartitions = [(partition, dataFrame.where(dataFrame[BHV['partitonColumn']] == partition)) for partition in uniqPartitions]
    print "allPartitions",allPartitions

    for (partition, partitionDF) in allPartitions:
        partitionDir = BHV['postload'] % (tableName, partition)
        print "partitionDir",partitionDir
        partitionDF.show()
        partitionDF.repartition(1).write.mode('append').parquet(partitionDir)
        refreshImpala(BHV['database'] + "." + tableName, partition, partitionDir, 'STAGE')

        factPartDir = BHV['factLoad'] % (tableName, partition)
        factPartDF = sqlContext.read.parquet(partitionDir)
        factPartDF = factPartDF.dropDuplicates(BHV['uniqKey'])
        factPartDF.repartition(1).write.mode('overwrite').parquet(factPartDir)
        refreshImpala(BHV['database'] + "." + tableName, partition, factPartDir, 'FACT')
        #refreshImpala(BHV['database'] + "." + tableName, partition, factPartDir)

# Main - ETL process start here
def main(sc, sqlContext):
    eTime = getStartEpoch()
    try:
        apnRDD = sc.textFile(BHV['apnDir'])
        apnsList = apnRDD.map(lambda x: (x.split(BHV['delimiter'])[2], x.split(BHV['delimiter'])[1])).collect()
        BHV['apnDict'] = {}
        for apn in apnsList:
            BHV['apnDict'][apn[0]] = apn[1]
    except:
        pass
    rawLogs = sc.textFile(BHV['preload'])

    #filterRDD = rawLogs.map(lambda x: x.split('NOT SET', 1)[1]).filter(lambda x: x.split('|')[0] in (['Prompt-and-Collect','Segment','inbound','outbound','Data Request']))
    #prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' and int(x.split('|')[1]) > eTime)
    #prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    #prepData.cache()

    #filterRDD = rawLogs.filter(lambda x: len(x.split('notused', 1)) > 1 ).map(lambda x:  x.split('notused\x00', 1)[1])
    #prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    #callFactLogs = prepData.filter(lambda x: x.split(BHV['delimiter'])[0] in (['Inbound','Outbound','inbound','outbound']))

    filterRDD = rawLogs.filter(lambda x:  x.split('|', 1)[0].lower().split('\x00')[-1] in ('inbound','outbound')).map(lambda x:x.split('\x00')[-1])
    callFactLogs = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    callFactLogs = callFactLogs.map(lambda x: parseInputRecord(x, [1,7,8], 22))
    callFactLogs = callFactLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2)
    callFactLogs.collect()
    processTable(BHV['cstbhv_call_fact'], "cstbhv_call_fact", callFactLogs)

    filterRDD = rawLogs.filter(lambda x:  x.split('|', 1)[0].lower().split('\x00')[-1] in ('bridge xfer','network xfer')).map(lambda x:x.split('\x00')[-1])
    callFactLogs = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    callFactLogs = callFactLogs.map(lambda x: parseInputRecord(x, [1,8,9], 23))
    callFactLogs = callFactLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2)
    callFactLogs.collect()
    processTable(BHV['cstbhv_call_fact_transfer'], "cstbhv_call_fact_transfer", callFactLogs)

    filterRDD = rawLogs.filter(lambda x: len(x.split('Prompt-and-Collect', 1)) > 1 ).map(lambda x: 'Prompt-and-Collect'+ x.split('Prompt-and-Collect', 1)[1])
    prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    pncFactLogs = prepData.filter(lambda x: x.split(BHV['delimiter'])[0] in ['Prompt-and-Collect'])
    pncFactLogs = pncFactLogs.map(lambda x: parseInputRecord(x, [1,6,7], 22))
    pncFactLogs = pncFactLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2)
    pncFactLogs.collect()
    processTable(BHV['cstbhv_prompt_collect_fact'], "cstbhv_prompt_collect_fact", pncFactLogs)

    filterRDD = rawLogs.filter(lambda x: len(x.split('Data Request', 1)) > 1 ).map(lambda x: 'Data Request'+ x.split('Data Request', 1)[1])
    prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    dataReqLogs = prepData.filter(lambda x: x.split(BHV['delimiter'])[0] in ['Data Request'])
    dataReqLogs = dataReqLogs.map(lambda x: parseInputRecord(x, [1,6,7], 19))
    dataReqLogs = dataReqLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2)
    dataReqLogs.collect()
    processTable(BHV['cstbhv_data_request'], "cstbhv_data_request", dataReqLogs)

    filterRDD = rawLogs.filter(lambda x: len(x.split('Segment', 1)) > 1 ).map(lambda x:  'Segment'+x.split('Segment', 1)[1])
    prepData = filterRDD.filter(lambda x: len(x.split('|')) > 3 and x.split('|')[1] <> '' )
    segmentFactLogs = prepData.filter(lambda x: x.split(BHV['delimiter'])[0] in ['Segment'])
    segmentFactLogs = segmentFactLogs.map(lambda x: parseInputRecord(x, [1,7], 15))
    segmentFactLogs.collect()
    #segmentFactLogs = segmentFactLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2).cache()
    segmentFactLogs = segmentFactLogs.filter(lambda (x1,x2): x1 == 'GOOD').map(lambda (x1,x2): x2)
    processTable(BHV['cstbhv_segment'], "cstbhv_segment", segmentFactLogs)

    prepData.unpersist()
    #segmentFactLogs.unpersist()


if __name__ == "__main__":

    # Configure OPTIONS
    conf = SparkConf().setAppName("etlBHVDispatcher")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    try:
        # Check Arguments
        checkArguments()

        # Execute Main functionality
        main(sc, sqlContext)
    finally:
        if 'configFile' in BHV and os.path.isfile(BHV['configFile']):
            os.remove(BHV['configFile'])
        if 'keytabLocalFile' in BHV and os.path.isfile(BHV['keytabLocalFile']):
            os.remove(BHV['keytabLocalFile'])

    sc.stop()
