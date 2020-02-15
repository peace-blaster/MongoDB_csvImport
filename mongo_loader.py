##########################
##      csvImport:      ##
##########################
#this should generically import data into MongoDB via python in a reasonable way.
#This is the function edition, to facilitate calling this script in other programs
#just call 'csvImport(path,dbInfo)', with 'path' as a path to the .csv, and dbInfo as a dict shaped like so:
#{
#    "IP": <domain/IP/URL/whatever for db server>
#    "dbName": <name of database in MongoDB server>
#    "dbCol": <name of collection you would like to append this to. It'll make a new one if dbCol doesn't exist>
#}

#######################
##      SOURCE:      ##
#######################

#import the data in Pandas
def importData(path):
    try:
        try:
            import pandas as pd
        except:
            raise ValueError('You need the Pandas module for this to work.')
        import pandas as pd
        dat=pd.read_csv(path)
        return dat
    except:
        raise ValueError('CSV file not found. Double check the path, and that the file exists.')
#make it database compliant
def fixBadColumns(df):
    df.columns=df.columns.str.strip().str.lower().str.replace(' ','_').str.replace('(','').str.replace(')','')
    return df
#make DB link
def dbConnect(IP, dbName, colName):
    try:
        try:
            import pymongo
        except:
            raise ValueError('You need the PyMongo module for this to work.')
        myclient=pymongo.MongoClient("mongodb://"+IP+":27017/")
        mydb=myclient[dbName]
        mycol=mydb[colName]
        return mycol
    except:
        raise ValueError('Something went wrong connecting to the DB.')
#load up the data to send (this is probably a bad way to do this, for future reference)
def insertDoc(doc, destCol, i):
    try:
        x=destCol.insert_many(doc)
    except:
        raise ValueError('Problem loading row '+i+'. Other values should be fine.')
def loadData(dat, destCol):
    #break it up for good data form, as well as memory efficiency for the script
    for i in range(len(dat[dat.columns[1]])):
        datRow=dat.iloc[[i]]
        datRow=datRow.to_dict('records')
        try:
            insertDoc(datRow,destCol,i)
        except:
            raise
#actually do it
def csvImport(path,dbInfo):
    dat=importData(path)
    dat=fixBadColumns(dat)
    mycol=dbConnect(dbInfo["IP"],dbInfo["dbName"],dbInfo["dbCol"])
    loadData(dat,mycol)

dbInfo={
    "IP": "192.168.1.11",
    "dbName": "mydatabase",
    "dbCol": "whatever"
}
path="main_tbl_202002142204.csv"
csvImport(path,dbInfo)
