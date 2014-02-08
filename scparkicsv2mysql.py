'''
scparkicsv2mysql
Created on Feb 7, 2014
@author: jay venti

This Program loads Sacramento City parking space data into a specified MySql database from a csv. 
It either clears or creates the parki_space table and then executes a MySQL LOAD DATA operation.
This data is specific to the data layout format of the PARKI-SPACE csv format found at:
http://data.cityofsacramento.org/datastreams/80139/locations-of-public-parking-spaces/
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
parki_spacetn   = 'parki_space2' #tablename name to load
csvfname        = "D:\Storage\\Project and Reasch\\Work Consulting\\Code Sacramento\\sac city apis\\gen-PARKI-SPACE.1.csv"
        

def setscParkiScaceCSV():
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

def sqlClearOrCreateScPSTable(dbhandle):
    results = 'FAILED'
    cursor = dbhandle.cursor()
    query = (
    "DROP TABLE IF EXISTS "+parki_spacetn+";\n" )
    #print query
    
    cursor.execute(query)
    dbhandle.commit()
    
    query = (
    "CREATE TABLE "+parki_spacetn+" (\n"
    "`idparki_space` int(11) NOT NULL AUTO_INCREMENT,\n"
    "`Name` varchar(12) DEFAULT NULL,\n"
    "`Latitude` decimal(16,13) DEFAULT NULL,\n"
    "`Longitude` decimal(16,13) DEFAULT NULL,\n"
    "`Altitude` decimal(16,13) DEFAULT NULL,\n"
    "`GISOBJID` int(11) DEFAULT NULL,\n"
    "`ADDRESS` int(11) DEFAULT NULL,\n"
    "`AORB` varchar(4) DEFAULT NULL,\n"
    "`STREET` varchar(15) DEFAULT NULL,\n"
    "`SUFFIX` varchar(6) DEFAULT NULL,\n"
    "`PREFIX` varchar(6) DEFAULT NULL,\n"
    "`EVENODD` varchar(5) DEFAULT NULL,\n"
    "`TIMELIMIT` varchar(18) DEFAULT NULL,\n"
    "`DAYRESTRIC` varchar(10) DEFAULT NULL,\n"
    "`PKGTYPE` varchar(34) DEFAULT NULL,\n"
    "`AORP` varchar(24) DEFAULT NULL,\n"
    "`PERMITAREA` varchar(4) DEFAULT NULL,\n"
    "`ROUTE` varchar(4) DEFAULT NULL,\n"
    "`ZONE` varchar(4) DEFAULT NULL,\n"
    "`MAXRATE` varchar(4) DEFAULT NULL,\n"
    "`SHAPE` varchar(10) DEFAULT NULL,\n"
    "`SHAPE_FID` int(11) DEFAULT NULL,\n"
    "`POINT_X` decimal(12,7) DEFAULT NULL,\n"
    "`POINT_Y` decimal(12,7) DEFAULT NULL,\n"
    "PRIMARY KEY (`idparki_space`)\n"
    ") ENGINE=InnoDB AUTO_INCREMENT=65536 DEFAULT CHARSET=latin1;\n" )
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

def sqlLodeDateFromCsv(dbhandle, scParkiScaceCSV):
    results = 'FAILED'
    cursor = dbhandle.cursor()
    
    query = (
    "LOAD DATA\n"
   "INFILE '"+scParkiScaceCSV+"'\n"
   "INTO TABLE parki_space."+parki_spacetn+"\n"  
       "FIELDS TERMINATED BY ','\n"
       "IGNORE 1 LINES\n"
       "(@var1, @var2, @var3, @var4, @var5, @var6, @var7, @var8, @var9, @var10,\n"
        "@var11, @var12, @var13, @var14, @var15, @var16, @var17, @var18, @var19, @var20,\n"
        "@var21, @var22, @var23)\n"
   "SET Name      =  @var1 ,\n"
       "Latitude  =  CONVERT( REPLACE(@var2, '"+'"'+"', '') , DECIMAL(18,12)),\n"
       "Longitude =  CONVERT( REPLACE(@var3, '"+'"'+"', '') , DECIMAL(18,12)),\n"
       "Altitude  =  CONVERT( REPLACE(@var4, '"+'"'+"', '') , DECIMAL(3,1)),\n"
       "GISOBJID  =  CONVERT( REPLACE(@var5, '"+'"'+"', '') , UNSIGNED),\n"
       "ADDRESS   =  CONVERT( REPLACE(@var6, '"+'"'+"', '') , UNSIGNED),\n"
       "AORB      =  REPLACE(@var7, '"+'"'+"', ''),\n"
       "STREET    =  REPLACE(@var8, '"+'"'+"', ''),\n"
       "SUFFIX    =  REPLACE(@var9, '"+'"'+"', ''),\n"
       "PREFIX    =  REPLACE(@var10, '"+'"'+"', ''),\n"
       "EVENODD   =  REPLACE(@var11, '"+'"'+"', ''),\n"
       "TIMELIMIT =  REPLACE(@var12, '"+'"'+"', ''),\n"
       "DAYRESTRIC=  REPLACE(@var13, '"+'"'+"', ''),\n"
       "PKGTYPE   =  REPLACE(@var14, '"+'"'+"', ''),\n"
       "AORP      =  REPLACE(@var15, '"+'"'+"', ''),\n"
       "PERMITAREA=  REPLACE(@var16, '"+'"'+"', ''),\n"
       "ROUTE     =  REPLACE(@var17, '"+'"'+"', ''),\n"
       "ZONE      =  REPLACE(@var18, '"+'"'+"', ''),\n"
       "MAXRATE   =  REPLACE(@var19, '"+'"'+"', ''),\n"
       "SHAPE     =  REPLACE(@var20, '"+'"'+"', ''),\n"
       "SHAPE_FID =  CONVERT( REPLACE(@var21, '"+'"'+"', '') , UNSIGNED),\n"
       "POINT_X   =  CONVERT( REPLACE(@var22, '"+'"'+"', '') , DECIMAL(12,7)),\n"
       "POINT_Y   =  CONVERT( REPLACE(@var23, '"+'"'+"', '') , DECIMAL(12,7));\n"     )
    #print query
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
    scParkiScaceCSV = setscParkiScaceCSV()
    print 'scParkiScaceCSV =',scParkiScaceCSV
    dbhandle = mysqlConnect(myhost,uid,pw,dbname)
    if dbhandle != None:
        if sqlClearOrCreateScPSTable(dbhandle) == 'SUCCESSFUL':
            sqlresults  =  sqlLodeDateFromCsv(dbhandle, scParkiScaceCSV )
            if sqlresults != 'FAILED':
                print 'sql load results: ',sqlresults
            else: print 'load unsuccessful, sql reports:',sqlresults
        else: print 'unable to drop or create the '+parki_spacetn+' table' 
    else: print 'unable to connect to database, exiting'
    
        
if __name__ == '__main__':
    main()