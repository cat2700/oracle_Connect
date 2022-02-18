import cx_Oracle
import threading
import os
import subprocess
import time


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
        finally:
            self.close_connect()

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

    def convertToXML(self, sFil):

        dfSource = pn.read_excel(sFil)

        print(dfSource)
        print(len(dfSource))
        print(dfSource.fillna(value=''))

        colList = ['CUS_CIVIL_NO', 'a', 'b', 'CUS_NAM_L', 'c', 'CUS_BIRTHDAY',
                   'BIRTH_GOV_COD', 'CBE_GENDER', 'ID_GOV_COD', 'CBE_NATIONAL_ALPHA']
        mapList = ['nationalId', 'secondaryId', 'secondaryIdType', 'arabicName', 'englishName',
                   'birthDate', 'birthGovCode', 'gender', 'residenceGovCode', 'nationality']
        rowlabel = ['customers', 'customer']

        filter = dfSource.loc[:, colList].fillna(
            value='').to_numpy().astype(str)

        print(filter)
        print(type(filter))

        row = ''
        allRows = ''
        # ['28807241402556' 'احمد جابرعليوه سالم ضيف' '19880724']
        for u in filter:    # catch rows
            x = 0
            row = ''
            # ['nationalId' ,'arabicName' ,'birthDate']
            for c in mapList:   # catch columns
                row += f'<{c}>' + u[x] + f'</{c}>'
                x += 1

            row = f'<{rowlabel[1]}>' + row + f'</{rowlabel[1]}>'
            allRows += row

        parent = f'<{rowlabel[0]}>' + allRows + f'</{rowlabel[0]}>'

        # create header
        bankCode = r'<bankCode>8201</bankCode>'
        date = r'<month>' + dt.today().strftime("%Y%m") + r'</month>'
        count = r'<noOfCustomers>' + \
            str(len(dfSource)) + r'</noOfCustomers>'

        head = f'<header>' + bankCode + date + count + f'</header>'

        # create body
        body = r'<document xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">' + \
            head + parent + r'</document>'

        # create all file
        uniCode = r'<?xml version="1.0" encoding="utf-8"?>' + body

        print(uniCode)
        print(type(uniCode))
        # formating
        x = et.XML(uniCode)
        et.indent(x)
        pretty_xml = et.tostring(x, encoding='utf8')
        # return
        # ucode = uniCode.encode("utf8")
        rrr = rn.randint(10000, 99999)
        pyFile = open(f'{rrr}.xml', 'wb')
        pyFile.write(pretty_xml)
        pyFile.close()

    # end

    # cn = my_conn(uid='arabank', upsw='icl', saved_dns_name="oracl2k")
    # print(cn.open_connect())
    # # print(cn.close_connect())
    # print(cn.runSQL("select accountid from temp_dep"))

    # cursor = connection.cursor()
    # print()
    # cursor = connection.cursor()
    # cursor.execute("""
    #         SELECT first_name, last_name
    #         FROM employees
    #         WHERE department_id = :did AND employee_id > :eid""",
    #         did = 50,
    #         eid = 190)
    # sql = 'select b.branch_no,b.bal_acc_no from arabank.bal_cr_tab b where b.cus_no=64328   and b.branch_no=919002000'
    # cursor.execute(sql)
    # for fname, lname in cursor:
    #     print("Values:", fname, lname)
    # connection.close()
    # print(os.getcwd()+'\\')
