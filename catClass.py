import cx_Oracle
import threading
import os
import subprocess
import time
import pandas as pn
import numpy as np
import random as rn
import datetime as dati
from datetime import date as dt
import xml.etree.ElementTree as et
# from xml.etree import ElementTree
import xml.dom.minidom as xxx
# from lxml import etree
from os.path import isfile, join
from os import listdir
# from xml.dom.minidom import parseString
from datetime import datetime
from cryptography.fernet import Fernet
import math


class mainClass:

    connection = None
    rows = []
    rowsLen = 0

    def __init__(self, uid='', upsw='', service_name='', ip='127.0.0.1', port='1521', saved_dns_name=''):
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

    def runSQL(self, SQLst):
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
                    cursor.execute(SQLst)
                    self.connection.commit()
                elif SQLst.lstrip().lower()[:2] in ('dr', 'cr', 'be'):
                    cursor.execute(SQLst)
                    self.connection.commit()
                return True, self.rowsLen, self.rows
        except Exception as err:
            return False, str(err), err.args
            print(False, str(err), err.args)
        finally:
            self.close_connect()

    def insertMany(self, SQLst, values=[], bulkRowsLimit=100000):
        try:
            if SQLst == '':
                return False
            else:
                self.open_connect()
                cursor = self.connection.cursor()
                if SQLst.lstrip().lower()[:2] in ('in') and len(values) > 0:
                    if len(values) > bulkRowsLimit:
                        while True:
                            print(
                                f'starting insert {len(values[:bulkRowsLimit])}')
                            cursor.executemany(SQLst, values[:bulkRowsLimit])
                            self.connection.commit()
                            values = values[bulkRowsLimit:]
                            if len(values) < bulkRowsLimit:
                                break
                    print('start insert')
                    cursor.executemany(SQLst, values)
                    self.connection.commit()
                    return True

        except Exception as err:
            return False, str(err), err.args
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

        def ExpOrcl(sqlST):
            print('starting ExpOrcl')

            rs = self.runSQL(sqlST)
            if not rs[0]:
                return False, 0

            rc = rs[1]
            filter = rs[2]
            # print(type(filter))
            # print(filter)
            return filter, rc

        def aftr_orcl_exprt(da):
            print('starting aftr_orcl_exprt')
            convAnd = 0  # to store count of cells who convert the & in it
            # convert tuple to list
            for indx, item in enumerate(da):
                da[indx] = list(da[indx])

            # convert none and dates
            nt = type(None)
            for o in da:
                for indx, item in enumerate(o):
                    if type(item) is nt:
                        o[indx] = ''
                    elif isinstance(item, dati.datetime):
                        y = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").year
                        m = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").month
                        d = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").day

                        o[indx] = str(y) + str(m).zfill(2) + str(d).zfill(2)
                    elif isinstance(item, str) and str(item).find('&') != -1:
                        A = item.replace(r"&", r"-")
                        o[indx] = A
                        convAnd += 1

            if convAnd > 0:
                print(f'Converted cells whith & is : {convAnd}')
            return da

        # ==> getAllExcelFiles

        def getAllExcFiles(exten=('xls', 'xlsx')):
            print('starting getAllExcFiles')
            fils = []
            mypath = os.curdir
            for f in listdir(mypath):
                if isfile(join(mypath, f)) and f.endswith(exten):
                    fils.append(f.split("."))
            return fils

        # ==> export data from excel file
        def ExpExl(FPathName):
            print('starting ExpExl')
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
            print('starting devid_Data')
            dfs = []
            rc = len(df)
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
            print('starting preExportXML')
            # ==> set rows before export xml
            if (rc <= maxRowsNum and maxRowsNum == 0) or maxRowsNum == 0:
                # exportXML(df)
                return df
            else:
                # devid_Data(df)
                return devid_Data(df)

        # ==> Import data to XML file
        def exportXML(Data, uniqNam):
            print('starting exportXML')

            # row = ''
            # baseData = ''

            # for Da in Data:  # ==> loop rows
            #     x = 0
            #     row = ''
            #     for c in mapList:  # ==> loop columns
            #         row += f'<{c}>' + str(Da[x]) + f'</{c}>'
            #         x += 1

            #     row = f'<{rowlabel[1]}>' + row + f'</{rowlabel[1]}>'
            #     baseData += row

            # parent = f'<{rowlabel[0]}>' + baseData + f'</{rowlabel[0]}>'
            parent = netXML(Data)
            # print(parent)
            # ==> create header
            count = f'{headerTAG[4]}' + str(len(Data)) + f'{headerTAG[5]}'
            head = f'{headerTAG[0]}' + f'{headerTAG[2]}' + \
                f'{headerTAG[3]}' + count + f'{headerTAG[1]}'

            # ==> create doc
            doc = f'{docTAG[0]}' + head + parent + f'{docTAG[1]}'
            # print(doc)

            # ==> create all file
            xmlFile = f'{unicodeTAG}' + doc

            # xml = xxx.parseString(xmlFile)
            # xml_pretty_str = xml.toprettyxml()

            # example = parseString(xmlFile).toprettyxml()
            # with open('file.xml', 'w') as file:
            #     file.write(example)
            # print(xmlFile)
            # print(type(xmlFile))

            # ==> formating

            x = et.XML(xmlFile)

            # parser = et.XMLParser(encoding="utf-8")
            # parser = etree.XMLParser(recover=True)

            # x = et.fromstring(xmlFile, parser=parser)
            # x = etree.fromstring(xmlFile, parser=parser)

            et.indent(x)

            pretty_xml = et.tostring(
                x, encoding='utf8', short_empty_elements=False)
            # pretty_xml = xmlFile.encode("utf8")

            rrr = rn.randint(10000, 99999)
            pyFile = open(f'{uniqNam}_{rrr}.xml', 'wb')
            pyFile.write(pretty_xml)
            pyFile.close()

        def netXML(Data):
            print('starting netXML')

            A = et.Element(rowlabel[0])
            for rows in Data:
                B = et.SubElement(A, rowlabel[1])
                # store count of fieldes
                f, t = 0, len(Data[0])
                while f < t:
                    et.SubElement(B, mapList[f]).text = str(rows[f])
                    f += 1

            tree = et.ElementTree(A)
            tree.write('temp.xml', encoding='utf-8',
                       xml_declaration=True, short_empty_elements=False)
            cd = os.curdir + r"\temp.xml"
            rf = open(cd, 'r', encoding="utf-8")
            allData = rf.read()
            rf.close()
            if os.path.exists("temp.xml"):
                os.remove("temp.xml")
            rem = r"<?xml version='1.0' encoding='utf-8'?>"
            netDataXML = allData.replace(rem, "")
            # f = open("demofile2.txt", "a")
            # f.write(netData)
            # f.close()
            return netDataXML
        # ====================================================
        # ==> Start run coding ::
        # ====================================================
        # ==> kind should be in [cust  or acc or card]
        if kind.strip().lower() == 'cust':
            rowlabel = ['customers', 'customer']
            mapList = ['nationalId', 'secondaryId', 'secondaryIdType', 'arabicName', 'englishName',
                       'birthDate', 'birthGovCode', 'gender', 'residenceGovCode', 'nationality']
        elif kind.strip().lower() == 'acc':
            rowlabel = ['accounts', 'account']
            mapList = ['accountId', 'typeId', 'currencyId', 'branchId', 'isJoint',
                       'openingDate', 'nationalId', 'secondaryId', 'secondaryIdType', 'closingDate', 'statusId', 'statusReason']
        elif kind.strip().lower() == 'card':
            rowlabel = ['cards', 'card']
            mapList = ['cardId', 'typeId', 'cardSchemaId', 'branchId', 'currencyId',
                       'accountId', 'openingDate', 'nationalId', 'secondaryId', 'secondaryIdType',
                       'parentCardId', 'parentNationalId', 'parentSecondryId', 'parentSecondryIdType',
                       'closingDate', 'statusId', 'statusReason']
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
            print('start from excel')
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
                        if maxRowsNum == 0:
                            exportXML(temp2, fi[0].lower())
                        else:
                            for d in temp2:
                                exportXML(d, fi[0].lower())
                else:
                    temp = ExpExl(spl[-1])
                    df, rc = temp[0], temp[1]
                    temp2 = preExportXML(df, rc)
                    if maxRowsNum == 0:
                        exportXML(temp2, spl[-1].split('.')[0].lower())
                    else:
                        for d in temp2:
                            exportXML(d, spl[-1].split('.')[0].lower())

        elif fromOrcl and sql != '':
            print('start from oracl')
            tmp = ExpOrcl(sql)
            tmp1, tmp2 = tmp[0], tmp[1]
            # chick for columns count
            if len(tmp1[0]) != len(mapList):
                print(
                    f'Sorry return {len(tmp1[0])} columns dont equal {len(mapList)} map columns')
                return
            # print(tmp1)
            # print(tmp2)
            df = aftr_orcl_exprt(tmp1)
            temp2 = preExportXML(df, tmp2)
            if maxRowsNum == 0:
                exportXML(temp2, '')
            else:
                for d in temp2:
                    exportXML(d, '')
        print('finished')

    def encrOrDecr(self, msg, decrpt=False):
        try:
            key = 'E7lFOU4SfA5Ifat9A44xyK0EcSgQvEXcuI4bfYQf6NI='

            def encrypt_message(message):
                """
                Encrypts a message
                """
                encoded_message = message.encode()
                f = Fernet(key)
                encrypted_message = f.encrypt(encoded_message)
                return encrypted_message

            def decrypt_message(encrypted_message):
                """
                Decrypts an encrypted message
                """
                f = Fernet(key)
                decrypted_message = f.decrypt(encrypted_message)
                return decrypted_message.decode()

            if not decrpt:  # Encrypt Mode
                return encrypt_message(msg).decode('utf_8')
            elif decrpt:  # Decrypt Mode
                return decrypt_message(msg.encode('utf_8'))
        except Exception as err:
            return str(err), err.args

    def ReadExcelFilesintoOrcl(self, readKind='', encryptedData=False):
        # Folders Details

        def kind(k):
            return {
                'a': ['WA-path', 'WA-sql', 'WA_columnsHeader'],
                'b': ['WB-path', 'WB-sql', 'WB_columnsHeader'],
            }.get(k, None)

        def readConfig(tags=[]):
            try:
                cd = os.curdir + r"\config.txt"
                rf = open(cd, 'r')
                all = rf.read().splitlines()
                if len(tags) == 0:
                    return all
                else:
                    res = []
                    for indx, item in enumerate(all):
                        if all[indx].split('||')[0].strip() in tags:
                            res.append(all[indx].split('||')[1].strip())
                    return res
            except Exception as err:
                return False
            finally:
                rf.close()

        # ==> getAllExcelFiles
        def getAllExcFiles(FolPath, exten=('xls', 'xlsx')):
            try:
                fils = []
                if not str(FolPath).endswith("\\"):
                    fb = str(FolPath) + "\\"
                else:
                    fb = str(FolPath)  # os.curdir
                for f in listdir(fb):
                    if isfile(join(fb, f)) and f.endswith(exten):
                        fils.append(fb + f)
                return fils
            except Exception as err:
                print(err)
                return fils

        def readExcel(fil, colmnList, sheetNam=0):
            dfSource = pn.read_excel(
                fil, usecols=colmnList, sheet_name=sheetNam)  # skiprows=range(1, 175000)
            # Rows Count
            rc = len(dfSource)

            # printq(dfSource)
            # print(type(dfSource))
            # print(len(dfSource))
            # dfSource.fillna(value='')
            # dfSource.astype(str)

            # .astype(str) # .fillna(value='')
            # filter = dfSource.loc[:, colmnList].to_numpy()
            filter = dfSource.loc[:, colmnList].fillna(
                value='').to_numpy().astype(str)
            # print(filter)
            # print(type(filter))

            return filter.tolist()

        """
            Starting Code
        """
        print('--')
        # if kind(readKind.lower()) == None:
        #     print('invalid kind')
        #     return
        # get folders Details
        # FData = readConfig(kind(readKind.lower()))
        # FolderPath = FData[0].strip()
        # sqlST = FData[1].strip()
        # columnsHeader = list(FData[2].strip().replace(' ', '').split(','))
        FolderPath = r'D:\GitHub\oracle_Connect\walletA'
        sqlST = 'insert into TEMP values (:1, :2, :3,:4,:5,:6,:7,:8,:9,:10)'
        columnsHeader = ['hashed_MD5',	'Activation_Date',	'status_id',	'Balance',
                         'National_ID',	'Mobile',	'terminal_id',	'bran',	'Last_TRX_Date', 'aa']
        # print(FolderPath, sqlST, columnsHeader)

        if len(columnsHeader) == 0:
            print(
                f'should enter columns Header {len(columnsHeader)} ')
            return False
        # => get all files in folder path
        fs = getAllExcFiles(FolPath=FolderPath)
        if len(fs) == 0:
            return False
        for f in fs:
            try:
                print(f'start with file: {f}')
                L = readExcel(fil=f, colmnList=columnsHeader)
                # print(L[:3])
                T = list([tuple(e) for e in L])
                # n = 100000
                # print(len(T))
                # if len(T) > n:
                #     while True:
                #         print(f'starting insert {len(T[:n])}')
                #         print(self.insertMany(sqlST, T[:n]))
                #         T = T[n:]
                #         if len(T) < n:
                #             break
                print('start insert')
                print(self.insertMany(sqlST, T))
                # print(T[:3])
                # for f in L[:3]:
                #     print(tuple(f))
                # print('start insert')
                # print(self.insertMany(sqlST, T))
                # print(self.insertMany(sqlST, L))
            except Exception as err:
                print(f'error in file: {f} and error msg is : {err}')

    def readConfig(self, configFileName, tags=[], isEncrypt=False):
        try:
            cd = os.curdir + f"\\{configFileName}.txt"
            rf = open(cd, 'r')
            all = rf.read().splitlines()
            if len(tags) == 0:
                return all
            else:
                res = []
                for indx, item in enumerate(all):
                    if all[indx].split('||')[0].strip() in tags:
                        res.append(all[indx].split('||')[1].strip())
                return res
        except Exception as err:
            return False
        finally:
            rf.close()

    def procShmool(self, fetchAll=True):
        try:
            # fixing datatype
            def fix(row):
                # convert none and dates
                nt = type(None)
                for indx, item in enumerate(row):
                    if type(item) is nt:
                        row[indx] = ''
                    elif isinstance(item, dati.datetime):
                        y = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").year
                        m = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").month
                        d = dati.datetime.strptime(
                            str(item), r"%Y-%m-%d %H:%M:%S").day

                        row[indx] = str(
                            y) + str(m).zfill(2) + str(d).zfill(2)
                    elif isinstance(item, str) and str(item).find('&') != -1:
                        A = item.replace(r"&", r"-")
                        row[indx] = A
                return row

            # process the procedure
            def process(row):
                try:
                    try:
                        # slicing row
                        row_rowID, row_br, row_cusNum, row_civil, row_birth, row_gov, row_rais, row_sex = \
                            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
                        # catch civil_no
                        row_civilLength = len(row_civil)
                        karn = row_civil[:1]
                        if karn == '2':
                            K = '19'
                        elif karn == '3':
                            K = '20'
                        else:
                            k = '99'
                        yy = row_civil[1:3]
                        mm = row_civil[3:5]
                        dd = row_civil[5:7]
                        try:
                            civilBirthDate = dt(
                                year=int(str(K) + str(yy).zfill(2)), month=int(str(mm).zfill(2)), day=int(str(dd).zfill(2)))
                        except Exception as er:
                            civilBirthDate = None
                        civil_sx = row_civil[12:13]
                        civil_gov = row_civil[7:9]
                        # catch birth day
                        try:
                            BirthDate = dt(
                                year=int(row_birth[:4]), month=int(row_birth[4:6]), day=int(row_birth[6:8]))
                        except Exception as er:
                            BirthDate = None
                        # catch gov code
                        gov = str(row_gov).zfill(2)
                        ris_gov = str(row_rais).zfill(2)
                        gov_tbl = ['01', '02', '03', '04', '05', '06', '11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '23', '24', '25', '26', '27', '28', '29', '31', '32', '33', '34', '35', '88'
                                   ]
                        # ==> chk digit
                        try:
                            first_round = int(row_civil[0:1]) * 2 + int(row_civil[1:2]) * \
                                7 + int(row_civil[2:3]) * 6 + int(row_civil[3:4]) * \
                                5 + int(row_civil[4:5]) * 4 + int(row_civil[5:6]) * \
                                3 + int(row_civil[6:7]) * 2 + int(row_civil[7:8]) * \
                                7 + int(row_civil[8:9]) * 6 + int(row_civil[9:10]) * \
                                5 + int(row_civil[10:11]) * 4 + int(row_civil[11:12]) * \
                                3 + int(row_civil[12:13]) * 2
                            current_chk_dig = int(row_civil[13:14])
                            chk_dig_civ = int(row_civil[13:14])
                            second_round = math.trunc(first_round/11)
                            third_round = second_round * 11 - first_round

                            if third_round == 0:
                                chk_dig = 1
                            else:
                                chk_dig = 11 + third_round
                        except Exception as er:
                            # print(False, str(er), er.args)
                            chk_dig = None

                    except Exception as er:
                        print(f'error in catching row: {row}')
                        return

                    # print(karn, yy, mm, dd, civilBirthDate, BirthDate, type(
                    #     civilBirthDate), civilBirthDate.year, civil_gov, gov)
                    # print("*" * 100)

                    # chk for 14 digit
                    if row_civilLength != 14:
                        error.append(('1', 'رقم بالرقم القومي - لا يساوي 14',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif karn not in ('2', '3'):
                        error.append(('2', 'خطا بالرقم القومي - رقم القرن',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif civilBirthDate == None or BirthDate == None:
                        error.append(('3', 'تاريخ الميلاد غير صحيح',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif civilBirthDate != BirthDate:
                        error.append(('4', 'تاريخ الميلاد المسجل لا يتطابق مع الرقم القومي',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif civilBirthDate.year > dt.today().year or (karn == '2' and civilBirthDate.year < 1920):
                        error.append(('5', 'خطا بالرقم القومي - سنة الميلاد',
                                      row_rowID, row_br, row_cusNum))
                        return
                    # التأكد من رقم المحافظة
                    elif civil_gov != gov:
                        error.append(('6', 'رقم محافظة الميلاد غير متطابق بالرقم القومي',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif civil_gov not in gov_tbl:
                        error.append(('7', 'رقم محافظة الميلاد غير موجود بجدول المحافظات',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif ris_gov not in gov_tbl:
                        error.append(('8', 'رقم محافظة السكن غير موجود بجدول المحافظات',
                                      row_rowID, row_br, row_cusNum))
                        return
                    # chick civile digit
                    elif chk_dig == None:
                        error.append(('9', 'رقم قومى يحتوى على حروف خاصة',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif chk_dig != chk_dig_civ:
                        error.append(('10', 'رقم قومى غير صحيح م',
                                      row_rowID, row_br, row_cusNum))
                        return
                    # sex
                    elif row_sex == '' or row_sex not in ('1', '2'):
                        error.append(('11', 'حقل الجنس غير صحيح',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif (int(civil_sx) % 2 == 0 and int(row_sex) != 2) or (int(civil_sx) % 2 != 0 and int(row_sex) != 1):
                        error.append(('12', 'حقل الجنس غير متوافق مع الرقم القومي',
                                      row_rowID, row_br, row_cusNum))
                        return
                    elif row_civil in civils:
                        error.append(('13', 'رقم قومى مكرر',
                                      row_rowID, row_br, row_cusNum))
                        return
                    else:
                        good.append((row_rowID, row_br, row_cusNum))
                        civils.append(row_civil)
                except Exception as er:
                    print('error in process row {row}')
                    return

            # drop and create good and errors tabels
            def dropAndCreateTBL():
                sql = """begin
                                execute immediate 'DROP TABLE customer_tab_error';
                                exception when others then
                                if sqlcode <> -942 then
                                    raise;
                                end if;
                            end; """
                self.runSQL(sql)
                sql = """create table customer_tab_error
                        (
                            errcode   CHAR(4),
                            errdesc   CHAR(200),
                            myrowid   ROWID,
                            branch_no NUMBER(10) not null,
                            cus_no    NUMBER(10) not null
                        )
                        """
                self.runSQL(sql)
                sql = """begin
                                execute immediate 'DROP TABLE customer_tab_good';
                                exception when others then
                                if sqlcode <> -942 then
                                    raise;
                                end if;
                            end; """
                self.runSQL(sql)
                sql = """create table customer_tab_good
                        (
                            myrowid   ROWID,
                            branch_no NUMBER(10) not null,
                            cus_no    NUMBER(10) not null
                        )
                        """
                self.runSQL(sql)

            def writelog(fileName, line):
                try:
                    cd = os.curdir + f"\\{fileName}"
                    rf = open(cd, 'w')
                    rf.write(str(line))
                except Exception as err:
                    print(str(err))
                finally:
                    rf.close()

            dat = '31/03/2022'
            cus_sql = f"""
                select c.rowid ,c.branch_no , c.cus_no ,c.cus_civil_no ,TRUNC(c.cus_birthday)
                    ,c.birth_gov_cod bgc ,c.id_gov_cod ,c.cus_sex
                    --, c.cus_nam_l,c.cus_addr_l,c.addr_pos_l,c.cus_typ
                    --,TRUNC(c.cus_id_dat),c.cus_activity ,to_char(c.cus_tel_no) cus_tel_no
                    --,to_char(c.mobil_no) mobil_no
                from customer_tab c
                where
                c.cus_kind = 0 and cus_opn_dat <= to_date('{dat}')
                and branch_no not in( 905001080,919009000)
                and   (branch_no,cus_no) in
                (

                    select branch_no , cus_no from bal_cr_tab b
                    where --bal_blnc >0 and
                    bal_close_dt is null

                    union

                    select  branch_no , cus_no from crt_bal c
                    where certif_flg in (1,2) and to_dat > '{dat}'

                    union

                    select  branch_no , cus_no from deposit_tab d
                    where dp_upd_dsg not in (2,4,6) and dp_dlt_dt is null and dp_val_dt > '{dat}'

                    union

                    select  branch_no , cus_no from LON_MASTER LS
                    where  LOAN_STATUS not in (2,6,9)

                    union
                    select branch_no , cus_no from bal_DB_tab DB
                    where  --bal_blnc >0 and
                    bal_close_dt is null

                )
                --and c.cus_civil_no not in (select cus_civil_no from CUSTOMER_TAB_shmol_rej)
                and c.cus_nationalt = 1
                and substr(c.cus_civil_no,8,2) in (select lpad(g.gov_id,2,0) from gov_tab g)
                and c.id_gov_cod in (select lpad(g.gov_id,2,0) from gov_tab g)
                and substr(c.cus_civil_no,8,2) = to_char(c.birth_gov_cod)
                and substr(c.cus_civil_no,2,6) = to_char(c.cus_birthday,'YYMMDD')
                --and c.branch_no in (910004000)
                --and rownum < 10
            """
            print('define cursor')
            error, good, civils = [], [], []
            print(self.open_connect())
            cursor = self.connection.cursor()
            cursor.prefetchrows = 1000001
            cursor.arraysize = 1000000
            cursor.execute(cus_sql)

            print('start fetch')
            # start fetch rows
            if fetchAll:
                rows = cursor.fetchall()
                print("starting loop in rows")
                for row in rows:
                    process(fix(list(row)))
                print(
                    f"good rows is : {len(good)} and errors rows is : {len(error)}")
            else:
                print("starting loop in rows")
                while True:
                    row = cursor.fetchone()
                    if row is None:
                        break
                    process(fix(list(row)))
                print(
                    f"good rows is : {len(good)} and errors rows is : {len(error)}")
            writelog("err.txt", error)
            writelog("good.txt", good)

            print('start dropAndCreateTBL')
            dropAndCreateTBL()
            # posting errors data
            print('posting errors data')
            ersql = """insert into customer_tab_error
                    values (:1,:2,:3,:4,:5) 
                    """
            print(self.insertMany(ersql, error))
            godsql = """insert into customer_tab_good
                    values (:1,:2,:3) 
                    """
            print(self.insertMany(godsql, good))

            print('Finished !!!')
        except Exception as err:
            print(f'error in cursor: {err}')
            return False
        finally:
            self.close_connect()

    def exportOrclExc(self, sql, excelName, sheetName):
        try:
            store = self.runSQL(sql)
            if not store[0]:
                print(store[0])
                return
            print(store[1], type(store[2]))
            df = pn.DataFrame(store[2])
            df.to_excel(excelName, sheet_name=sheetName)
            print('Expored !!!')
        except Exception as err:
            print(f'error in exportOrclExc: {err}')
            return False
