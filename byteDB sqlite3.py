from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
from sqlalchemy.sql.expression import exists

from sqlalchemy import text
import os
import os, time, datetime
from win32_setctime import setctime
import sys

import logging
logging.basicConfig(filename=("backupLog.txt"), level=logging.INFO, format='%(asctime)s: %(message)s')


import sqlite3
conn = sqlite3.connect('sqlite3.db')

meta = MetaData()

def _get_date():
    return datetime.datetime.now()


c = conn.cursor()
conn.execute('''CREATE TABLE IF NOT EXISTS codeDB
         (id INTEGER NOT NULL PRIMARY KEY,
         filename TEXT,
         path TEXT,
         data TEXT,
         createdTime TEXT,
         modifiedTime TEXT,
         fileSize TEXT);''')
conn.commit()




def convert_bytes(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
def file_size(file_path):
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)

def readFiles(): 
    def exlCompanyRun(companyData):
        loc = (companyData)
        tempName=companyData.split('\\')[-1]

        fob=open(loc,'r+')
        chk=conn.execute('SELECT EXISTS (SELECT * FROM codeDB WHERE filename = "'+tempName+'" and path = "'+loc+'") AS anon_1').fetchall()
        #print(chk)
        if chk[0][0]==0:
            fob=open(loc,'r+')
            created = os.path.getctime(loc)
            cccreated=(os.path.getctime(loc))
            modified = os.path.getmtime(loc)
            mmmodified=(os.path.getmtime(loc))
            fileSize=file_size(loc)
            tempText=''        
            for i in fob:
              tempText=tempText+i   
            conn.execute("INSERT INTO codeDB(filename,path,data,createdTime,modifiedTime,fileSize) values(?,?,?,?,?,?)" ,(tempName, loc, tempText,cccreated,mmmodified,fileSize))
           
            #print('Data Successfully Inserted')
            logging.info('\tData Successfully Inserted: '+str(loc))
        else:
            #print('Already Exists\n')
            logging.info('\tAlready Exists')
            
    # Getting the current work directory (cwd)
    chkCurrFile=True
    userPath=input('Press Enter for creating backup for current directory\nor Enter folder name to create database: ')
    if len(userPath)>0 and userPath!='.':
        thisdir = userPath
        logging.info('\tYou Enter Directory Name: '+userPath)
        chkCurrFile=False
    else:
        thisdir=os.getcwd()
        logging.info('\tYou select current directory: '+thisdir)
        
        
    root_dir = thisdir
    file_set = set()
    fileList=[]

    for dir_, _, files in os.walk(root_dir):
        for file_name in files:
            rel_dir = os.path.relpath(dir_, root_dir)
            #print(rel_dir)
            rel_file = os.path.join(rel_dir, file_name)
            fileList.append(rel_file)
            #print(rel_file)
            file_set.add(rel_file)

    allFiles=[]
    for i in fileList:
        if chkCurrFile==False:
            if i[0]=='.':
                allFiles.append(userPath+'\\'+i[2:])
            else:
                allFiles.append(userPath+'\\'+i)
        else:
            if i[0]=='.':
                    allFiles.append(i[2:])
            else:
                    allFiles.append(i)

    logging.info('\tAll File Names Filtered')
    print('\n')
    allFiles.remove('backupLog.txt')
    if chkCurrFile==True:
        try:
            allFiles.remove('byteDB sqlite3.py')
        except:
            pass
    else:
        print('Initiating Next Step')
        
        
    print('Total Files Inserting into database is : ',len(allFiles))
    logging.info('\t--------------- Writing Into Database Started ---------------------')
    print('--------------- Writing Into Database Started ---------------------')
    writeDbCount=0
    for i in allFiles:
        try:
            writeDbCount=writeDbCount+1
            sys.stdout.write('\r'+str(writeDbCount)+'\t\t '+str(i)+'                               ')
            exlCompanyRun(i)
            sys.stdout.flush()
        except:
            logging.info('\tError occured in: '+str(i))

    logging.info('\t============================================================')
    print('\n============================================================')
    input('\n\nYour task has been compleated sucessfully\nPress Enter to close the program...')





def writeFiles():
    logging.info('\t\n\n\n--------------- Writing Files Started ---------------------')
    print('\n\n\n--------------- Writing Files Started ---------------------')


    def excel_writer(filename,path,data,crtime,modifiedTime):
        temppath=path

        temppath=temppath.split('\\')
        if len(temppath)>1:
            try:
                os.makedirs(path.rsplit('\\',1)[0])
            except:
                pass
            
        fob=open(path,'w+')
        fob.write(data)
        fob.close()
        setctime(path, float(crtime))
        os.utime(path, (modifiedTime, modifiedTime))
        #print('Data Successfully Written')
        logging.info('\tData Successfully Written for: '+str(path))
        

    qw=conn.execute('SELECT * FROM codeDB').fetchall()
    writeSysCount=0
    for i in qw:
        writeSysCount=writeSysCount+1    
        sys.stdout.write('\r'+str(writeSysCount)+'\t\t '+str(i[2])+'                               ')
        excel_writer(i[1],i[2],i[3],i[4],float(i[5]))
        sys.stdout.flush()
        #pass
    print('\n============================================================')
    logging.info('\t============================================================')
    print("\n\nQuery Executed successfully")
    logging.info('\t\n\nQuery Executed successfully')
    input('\n\nYour task has been compleated sucessfully\nPress Enter to close the program...')



os.system("title File Backup by Sandeep kumar")
d_date = datetime.datetime.now()
reg_format_date = d_date.strftime("  %d-%m-%Y\t\t\t\t\t  File Backup\t\t\t\t\t  %I:%M:%S %p")
print ('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
print (reg_format_date)
print ('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')

n=input('Press 1 for Insert into Database Files\nPress 2 for write Files from Database\nEnter your choice: ')
if n=='1':
    readFiles()
    conn.commit()
elif n=='2':
    writeFiles()
else:
    print('Invalid Choice')
