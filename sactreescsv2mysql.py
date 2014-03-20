'''
sactreescsv2mysql.py
Created on Mar 20, 2014
@author: jay venti

This Program loads Sacramento localtree data into a specified MySql database from a csv. 
It either clears or creates the 'dbname.newTableName' table and then executes a MySQL LOAD DATA operation.
This data is specific to the data layout format of the PARKI-SPACE csv format found at:
http://data.cityofsacramento.org/datastreams/
As of 2014-01-07.
This data is in a custom junar klm like file format and is converted to flat format 
by jrcsv2gncsv.py included in this package.

Note 1 the code was designed for window pathnames, use on Linux may require modification.
Note 2 code does not create indexes you will probably want to build indexes for your specific purpose. 
'''

#from pprint import pprint
import sys
import mysql.connector
from mysql.connector import errorcode

myhost          = 'localhost' # '127.0.0.1'
uid             = 'root'
pw              = 'root'
dbname          = 'parki_space'
newTableName   = 'local_trees' #tablename name 
csvfname        = "D:\Storage\\Project and Reasch\\Work Consulting\\Code Sacramento\\sac city apis\\gen-locations-of-city-trees.csv"
        

def setsCSVfilenam():
    #set csv file name
    x = len(sys.argv) 
    if x < 2:
        ParkiScaceCSV = csvfname
    else:
        ParkiScaceCSV = sys.argv[x-1]
    ParkiScaceCSV = ParkiScaceCSV.replace('\\', '\\'+'\\')
    return ParkiScaceCSV

def mysqlConnect(myhost,uid,pw,dbname):
    sqlhandle = None
    try:
        sqlhandle = mysql.connector.connect(user= uid, password= pw, host= myhost, database= dbname)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists")
        else:
            print(err)
    else:
        print 'sqlhandle set'
    return sqlhandle

def sqlClearOrCreateTreeTable(dbhandle):
    results = 'FAILED'
    cursor = dbhandle.cursor()
    query = (
    "DROP TABLE IF EXISTS "+newTableName+";\n" )
    #print query
    
    cursor.execute(query)
    dbhandle.commit()
    
    query = (
    "CREATE TABLE "+dbname+"."+newTableName+" (\n"
    "`idlocal_trees` int(11) NOT NULL AUTO_INCREMENT,\n"
    "`Name` varchar(12) DEFAULT NULL, \n"
    "`Latitude` decimal(18,14) DEFAULT NULL, \n"
    "`Longitude` decimal(18,14) DEFAULT NULL,\n"
    "`Altitude` decimal(18,14) DEFAULT NULL,\n"
    "`GISOBJID` int(11) DEFAULT NULL,\n"
    "`OBJ_CODE` int(11) DEFAULT NULL,\n"
    "`ADDRESS_NUMBER` int(11) DEFAULT NULL,\n"
    "`STREET` varchar(30) DEFAULT NULL,\n"
    "`LANDUSE` varchar(30) DEFAULT NULL,\n"
    "`PLANTTYPE`varchar(30) DEFAULT NULL,\n"
    "`GROWSPACE` varchar(6) DEFAULT NULL,\n"
    "`CONDUCTOR` varchar(25) DEFAULT NULL,\n"
    "`HDSCAPE` varchar(30) DEFAULT NULL,\n"
    "`SPECIES` varchar(30) DEFAULT NULL,\n"
    "`BOTANICAL` varchar(40) DEFAULT NULL,\n"
    "`CULTIVAR` varchar(25) DEFAULT NULL,\n"
    "`STEMS` varchar(4) DEFAULT NULL,\n"
    "`DBH` varchar(12) DEFAULT NULL,\n"
    "`ROOT` varchar(10) DEFAULT NULL,\n"
    "`WOOD` varchar(4) DEFAULT NULL,\n"
    "`FOLIAGE` varchar(4) DEFAULT NULL,\n"
    "`SHAPE` varchar(10) DEFAULT NULL,\n"
    "`SHAPE_FID` int(11) DEFAULT NULL,\n"
    "`POINT_X` decimal(11,6) DEFAULT NULL,\n"
    "`POINT_Y` decimal(11,6) DEFAULT NULL,\n"
    "PRIMARY KEY (`idlocal_trees`)\n"
    ") ENGINE=InnoDB AUTO_INCREMENT=165789 DEFAULT CHARSET=latin1;\n"  )
    #print query
    try:
        cursor.execute(query)
    except mysql.connector.Error as err:    
        print(err)
    else:
        dbhandle.commit()
        cursor.close()
        results = 'SUCCESSFUL'
    return results

def sqlLodeDateFromCsv(dbhandle, CSVfileName):
    results = 'FAILED'
    cursor = dbhandle.cursor()
    #'"+'"+'"'+"'+"'
    query = (
   "LOAD DATA \n"
   " INFILE '"+CSVfileName+"' \n "
   "INTO TABLE "+dbname+"."+newTableName+"  \n "
   "    FIELDS TERMINATED BY ',' ENCLOSED BY '"+'"'+"' \n "
   "    IGNORE 1 LINES \n "
   "    (@var1, @var2, @var3, @var4, @var5, @var6, @var7, @var8, @var9, @var10, \n "
   " @var11, @var12, @var13, @var14, @var15, @var16, @var17, @var18, @var19, @var20, \n "
   " @var21, @var22, @var23, @var24, @var25)  \n "
   "SET Name      =  @var1 , \n "
   "    Latitude  =  CONVERT( REPLACE(@var2, '"+'"'+"', '') , DECIMAL(18,14)), \n "
   "    Longitude =  CONVERT( REPLACE(@var3, '"+'"'+"', '') , DECIMAL(18,14)), \n "
   "    Altitude  =  CONVERT( REPLACE(@var4, '"+'"'+"', '') , DECIMAL(3,1)), \n "
   "    GISOBJID  =  CONVERT( REPLACE(@var5, '"+'"'+"', '') , UNSIGNED), \n "
   "    OBJ_CODE  =  CONVERT( REPLACE(@var6, '"+'"'+"', '') , UNSIGNED), \n "
   "    ADDRESS_NUMBER   =  CONVERT( REPLACE(@var7, '"+'"'+"', '') , UNSIGNED), \n "
   "    STREET    =  REPLACE(@var8, '"+'"'+"', ''), \n "
   "    LANDUSE   =  REPLACE(@var9, '"+'"'+"', ''), \n "
   "    PLANTTYPE =  REPLACE(@var10, '"+'"'+"', ''), \n "
   "    GROWSPACE =  REPLACE(@var11, '"+'"'+"', ''), \n "
   "    CONDUCTOR =  REPLACE(@var12, '"+'"'+"', ''), \n "
   "    HDSCAPE   =  REPLACE(@var13, '"+'"'+"', ''), \n "
   "    SPECIES   =  REPLACE(@var14, '"+'"'+"', ''), \n "
   "    BOTANICAL =  REPLACE(@var15, '"+'"'+"', ''), \n "
   "    CULTIVAR  =  REPLACE(@var16, '"+'"'+"', ''), \n "
   "    STEMS     =  REPLACE(@var17, '"+'"'+"', ''), \n "
   "    DBH       =  REPLACE(@var18, '"+'"'+"', ''), \n "
   "    ROOT      =  REPLACE(@var19, '"+'"'+"', ''), \n "
   "    WOOD      =  REPLACE(@var20, '"+'"'+"', ''), \n "
   "    FOLIAGE   =  REPLACE(@var21, '"+'"'+"', ''), \n "
   "    SHAPE     =  REPLACE(@var22, '"+'"'+"', ''), \n "
   "    SHAPE_FID =  CONVERT( REPLACE(@var23, '"+'"'+"', '') , UNSIGNED), \n "
   "    POINT_X   =  CONVERT( REPLACE(@var24, '"+'"'+"', '') , DECIMAL(11,6)), \n "
   "    POINT_Y   =  CONVERT( REPLACE(@var25, '"+'"'+"', '') , DECIMAL(11,6)); \n "
  )
    print query
    
    try:
        cursor.execute(query)
    except mysql.connector.Error as err:    
        print(err)
    else:
        dbhandle.commit()
        cursor.close()
        dbhandle.close()
        results = 'SUCCESSFUL'
    
    return results

def main():
    CSVfileName = setsCSVfilenam()
    print 'CSVfileName =',CSVfileName
    dbhandle = mysqlConnect(myhost,uid,pw,dbname)
    if dbhandle != None:
        if sqlClearOrCreateTreeTable(dbhandle) == 'SUCCESSFUL':
            sqlresults  =  sqlLodeDateFromCsv(dbhandle, CSVfileName )
            if sqlresults != 'FAILED':
                print 'sql load results: ',sqlresults
            else: print 'load unsuccessful, sql reports:',sqlresults
        else: print 'unable to drop or create the '+newTableName+' table' 
    else: print 'unable to connect to database, exiting'
    
        
if __name__ == '__main__':
    main()
