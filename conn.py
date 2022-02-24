import cx_Oracle
import threading
import os
import subprocess
import time
import pandas as pn
import numpy as np
import random as rn
from datetime import date as dt
import xml.etree.ElementTree as et
from os.path import isfile, join
from os import listdir
# from xml.dom.minidom import parseString


class my_conn:

    connection = None
    rows = []
    rowsLen = 0

    def __init__(self, uid, upsw, service_name='', ip='127.0.0.1', port='1521', saved_dns_name=''):
        self.user = uid
        self.passw = upsw
        self.service = service_name
        self.ip = ip
        self.port = port
        self.saved_dns_name = saved_dns_name
        # self.new_dsn = new_dsn

    def open_connect(self):
        try:
            if self.saved_dns_name == '':
                dsn = self.ip + "/" + self.service
                self.connection = cx_Oracle.connect(user=self.user, password=self.passw,
                                                    dsn=dsn)
            elif self.saved_dns_name != '':
                self.connection = cx_Oracle.connect(user=self.user, password=self.passw, dsn=self.saved_dns_name,
                                                    encoding="UTF-8")
            # elif self.saved_dns_name == '' and self.new_dsn:
            #     dsn = cx_Oracle.makedsn(
            #         self.ip, 1521, service_name=self.service)
            #     self.connection = cx_Oracle.connect(user=self.user, password=self.passw, dsn=dsn,
            #                                         encoding="UTF-8")
            else:
                return False

            return True
        except BaseException as err:
            return False, err
        # finally:
            # self.close_connect()

    def close_connect(self):
        try:
            self.connection.close()
            return True
        except BaseException as err:
            return False, err

    def runSQL(self, SQLst, values=[]):
        try:
            if SQLst == '':
                return False
            else:
                self.open_connect()
                cursor = self.connection.cursor()
                if SQLst.lstrip().lower()[:2] in ('se'):
                    cursor.execute(SQLst)
                    self.rows = cursor.fetchall()
                    self.rowsLen = len(self.rows)
                elif SQLst.lstrip().lower()[:2] in ('in', 'de'):
                    if len(values) > 0:
                        cursor.executemany(SQLst, values)
                    else:
                        cursor.execute(SQLst)
                    self.connection.commit()
                return True, self.rowsLen, self.rows
        except BaseException as err:
            return False, err
        finally:
            self.close_connect()

    def backupORRestore(self, isBackup=False, isrestore=False, restoreFile=''):
        try:
            if not self.open_connect() or (not isBackup and not isrestore) or (isrestore and restoreFile == ''):
                return False
            savePath = os.getcwd()+'\\'

            print('starting whith my current Dir.' + savePath)

            def adminOrder():
                def run_sqlplus(sqlplus_script):
                    """
                    Run a sql command or group of commands against
                    a database using sqlplus.
                    """

                    p = subprocess.Popen(['sqlplus', '/nolog'], stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (stdout, stderr) = p.communicate(
                        sqlplus_script.encode('utf-8'))
                    stdout_lines = stdout.decode('utf-8').split("\n")

                    return stdout_lines

                sqlplus_script = f"""
                        CONN / AS SYSDBA
                        ALTER USER {self.user} IDENTIFIED BY {self.passw} ACCOUNT UNLOCK;
                        CREATE OR REPLACE DIRECTORY tst_dir AS '{savePath}';
                        GRANT READ, WRITE ON DIRECTORY tst_dir TO  {self.user};
                        exit
                    """

                sqlplus_output = run_sqlplus(sqlplus_script)

                for line in sqlplus_output:
                    print(line)

            t = threading.Timer(2.0, adminOrder)
            t.start()
            t._wait_for_tstate_lock()

            global base_command
            if isBackup:        # Backup
                base_command = f'expdp {self.user}/{self.passw}@{self.service} full=Y directory=tst_dir dumpfile='
                FN = time.strftime('%Y-%m-%d-%H-%M-%S')

            elif isrestore:     # Restore
                # Drop User and create again
                def run_sqlplus(sqlplus_script):
                    """
                    Run a sql command or group of commands against
                    a database using sqlplus.
                    """

                    p = subprocess.Popen(['sqlplus', '/nolog'], stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (stdout, stderr) = p.communicate(
                        sqlplus_script.encode('utf-8'))
                    stdout_lines = stdout.decode('utf-8').split("\n")

                    return stdout_lines
                    # --create user {self.user} identified by {self.passw} default tablespace;
                    #     --cra_dbs temporary tablespace temp quota unlimited on cra_dbs;
                print('O'*50)

                # Shutdown immediate;
                # startup restrict;
                sqlplus_script = f"""
                        conn / AS SYSDBA
                        drop user {self.user} cascade;


                        alter session set "_ORACLE_SCRIPT"=true;
	                    show con_name;

                        create user {self.user} identified by {self.passw} default tablespace CRA_DBS temporary tablespace TEMP profile DEFAULT;


                        grant create table to {self.user} ;
                        grant create trigger to {self.user} ;
                        grant create procedure to {self.user} ;
                        grant create sequence to {self.user} ;
                        grant create session to {self.user} ;
                        grant dba to {self.user};
                        alter user {self.user} quota unlimited on lon_dbs ;
                        alter user {self.user} quota unlimited on lon_idx ;
                        alter user {self.user} quota unlimited on cnv_dbs ;
                        alter user {self.user} quota unlimited on cnv_idx ;
                        alter user {self.user} quota unlimited on dep_dbs ;
                        alter user {self.user} quota unlimited on dep_idx ;
                        alter user {self.user} quota unlimited on cra_idx ;
                        alter user {self.user} quota unlimited on crt_idx ;
                        alter user {self.user} quota unlimited on crt_dbs ;
                        alter user {self.user} quota unlimited on tlr_dbs ;
                        alter user {self.user} quota unlimited on tlr_idx ;
                        alter user {self.user} quota unlimited on sig_dbs ;
                        alter user {self.user} quota unlimited on sig_idx ;
                        alter user {self.user} quota unlimited on gnl_dbs ;
                        alter user {self.user} quota unlimited on gnl_idx ;
                        alter user {self.user} quota unlimited on ilc_dbs ;
                        alter user {self.user} quota unlimited on ilc_idx ;
                        alter user {self.user} quota unlimited on cbl_dbs ;
                        alter user {self.user} quota unlimited on cbl_idx ;
                        alter user {self.user} quota unlimited on lgr_dbs ;
                        alter user {self.user} quota unlimited on lgr_idx ;
                        alter user {self.user} quota unlimited on col_dbs ;
                        alter user {self.user} quota unlimited on col_idx ;
                        alter user {self.user} quota unlimited on clr_dbs ;
                        alter user {self.user} quota unlimited on clr_idx ;
                        alter user {self.user} quota unlimited on gds_dbs ;
                        alter user {self.user} quota unlimited on gds_idx ;
                        alter user {self.user} quota unlimited on cmf_dbs ;
                        alter user {self.user} quota unlimited on cmf_idx ;
                        alter user {self.user} quota unlimited on lot_dbs ;
                        alter user {self.user} quota unlimited on lot_idx ;
                        alter user {self.user} quota unlimited on fst_dbs ;
                        alter user {self.user} quota unlimited on fst_idx ;
                        alter user {self.user} quota unlimited on bdg_dbs ;
                        alter user {self.user} quota unlimited on bdg_idx ;
                    """

                sqlplus_output = run_sqlplus(sqlplus_script)

                for line in sqlplus_output:
                    print(line)

                base_command = f'impdp {self.user}/{self.passw}@{self.service} full=Y directory=tst_dir dumpfile='
                FN = restoreFile

            def orclbk():
                command = base_command + FN + '.dmp'  # + 'logfile=expdpSCOTT.log'
                print(command)
                if os.system(command) == 0:
                    print('successful')
                else:
                    print('failed')

            t = threading.Timer(2.0, orclbk)
            t.start()

        except BaseException as err:
            return False, err
        finally:
            self.close_connect()

    def convertToXML(self, kind='', fromOrcl=False, fromEx=False,  filePathName='', sheetName=0, colList=[], sql='', maxRowsNum=0):

        def ExpOrcl():
            rs = self.runSQL(sql)
            if not rs[0]:
                return False

            rc = rs[1]
            filter = rs[2]
            print(type(filter))
            print(filter)

            return filter, rc

        # ==> getAllExcelFiles
        def getAllExcFiles(exten=('xls', 'xlsx')):
            fils = []
            mypath = os.curdir
            for f in listdir(mypath):
                if isfile(join(mypath, f)) and f.endswith(exten):
                    fils.append(f.split("."))
            return fils

        # ==> export data from excel file
        def ExpExl(FPathName):

            dfSource = pn.read_excel(
                FPathName, sheet_name=sheetName, usecols=colList, dtype=str)  # skiprows=range(1, 175000)
            # Rows Count
            rc = len(dfSource)

            # print(dfSource)
            # print(type(dfSource))
            # print(len(dfSource))
            # print(dfSource.fillna(value=''))

            filter = dfSource.loc[:, colList].fillna(
                value='').to_numpy().astype(str)

            # print(filter)
            # print(type(filter))

            return filter, rc

        # ==> deviding data
        def devid_Data(df):
            dfs = []
            circle = int((rc // maxRowsNum) + 1)
            net = int(rc % maxRowsNum)
            x = 1
            f, t = 0, maxRowsNum
            while x <= circle:
                if x < circle:
                    t = maxRowsNum * x
                else:
                    t += net
                # exportXML(df[f:t])
                dfs.append(df[f:t])
                f = t
                x += 1
            return dfs

        # preExportXml
        def preExportXML(df, rc):
            # ==> set rows before export xml
            if (rc <= maxRowsNum and maxRowsNum == 0) or maxRowsNum == 0:
                # exportXML(df)
                return df
            else:
                # devid_Data(df)
                return devid_Data(df)

        # ==> Import data to XML file
        def exportXML(Data, uniqNam):
            row = ''
            baseData = ''
            for Da in Data:  # ==> loop rows
                x = 0
                row = ''
                for c in mapList:  # ==> loop columns
                    row += f'<{c}>' + Da[x] + f'</{c}>'
                    x += 1

                row = f'<{rowlabel[1]}>' + row + f'</{rowlabel[1]}>'
                baseData += row

            parent = f'<{rowlabel[0]}>' + baseData + f'</{rowlabel[0]}>'

            # ==> create header
            count = f'{headerTAG[4]}' + str(len(Data)) + f'{headerTAG[5]}'
            head = f'{headerTAG[0]}' + f'{headerTAG[2]}' + \
                f'{headerTAG[3]}' + count + f'{headerTAG[1]}'

            # ==> create doc
            doc = f'{docTAG[0]}' + head + parent + f'{docTAG[1]}'
            # print(doc)

            # ==> create all file
            xmlFile = f'{unicodeTAG}' + doc

            # example = parseString(xmlFile).toprettyxml()
            # with open('file.xml', 'w') as file:
            #     file.write(example)
            # print(xmlFile)
            # print(type(xmlFile))
            # ==> formating
            x = et.XML(xmlFile)
            et.indent(x)
            pretty_xml = et.tostring(
                x, encoding='utf8', short_empty_elements=False)
            # return
            # pretty_xml = xmlFile.encode("utf8")
            rrr = rn.randint(10000, 99999)
            pyFile = open(f'{uniqNam}_{rrr}.xml', 'wb')
            pyFile.write(pretty_xml)
            pyFile.close()

        # ====================================================
        # ==> Start run coding ::
        # ====================================================
        # ==> kind should be in [cust  or acc or card]
        if kind.strip().lower() == 'cust':
            rowlabel = ['Customers', 'Customer']
            mapList = ['nationalId', 'secondaryId', 'secondaryIdType', 'arabicName', 'englishName',
                       'birthDate', 'birthGovCode', 'gender', 'residenceGovCode', 'nationality']
        elif kind.strip().lower() == 'acc':
            rowlabel = ['Accounts', 'Account']
            mapList = ['accountId', 'typeId', 'currencyId', 'branchId', 'isJoint',
                       'openingDate', 'nationalId', 'secondaryId', 'secondaryIdType', 'closingDate', 'statusId', 'statusReason']
        elif kind.strip().lower() == 'card':
            rowlabel = ['Cards', 'Card']
        else:
            print('please kind attr. required')
            return

        # ==> Tags
        unicodeTAG = r'<?xml version="1.0" encoding="utf-8"?>'
        docTAG = [
            r'<document xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">', r'</document>']
        headerTAG = [r'<header>', r'</header>', r'<bankCode>8201</bankCode>',
                     r'<month>' + dt.today().strftime("%Y%m") + r'</month>',
                     f'<noOf{rowlabel[0]}>', f'</noOf{rowlabel[0]}>']

        if fromEx:
            # ==> store filePathName as a list
            spl = list(filePathName.split('\\'))
            # ==> Extract from excel
            if len(colList) == len(mapList):
                # ==> define one file or more
                if spl[-1] == '*':
                    fils = getAllExcFiles()
                    for fi in fils:
                        F = fi[0] + '.' + fi[1]
                        temp = ExpExl(F)
                        df, rc = temp[0], temp[1]
                        temp2 = preExportXML(df, rc)
                        for d in temp2:
                            exportXML(d, fi[0].lower())
                else:
                    temp = ExpExl(spl[-1])
                    df, rc = temp[0], temp[1]
                    temp2 = preExportXML(df, rc)
                    for d in temp2:
                        exportXML(d, spl[-1].split('.')[0].lower())

        elif fromOrcl and sql != '':
            ExpOrcl()
            return
