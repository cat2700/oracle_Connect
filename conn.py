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
                sqlplus_script = f"""
                        conn / AS SYSDBA
                        drop user {self.user} cascade; 
                        
                        CREATE TABLESPACE BDG_DBS DATAFILE 'D:\ARABANK_DB\BDG_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE BDG_IDX DATAFILE 'D:\ARABANK_DB\BDG_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CBL_DBS DATAFILE 'D:\ARABANK_DB\CBL_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CBL_IDX DATAFILE 'D:\ARABANK_DB\CBL_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CLR_DBS DATAFILE 'D:\ARABANK_DB\CLR_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CLR_IDX DATAFILE 'D:\ARABANK_DB\CLR_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CMF_DBS DATAFILE 'D:\ARABANK_DB\CMF_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CMF_IDX DATAFILE 'D:\ARABANK_DB\CMF_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE COL_DBS DATAFILE 'D:\ARABANK_DB\COL_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE COL_IDX DATAFILE 'D:\ARABANK_DB\COL_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE BIGFILE TABLESPACE CRA_DBS DATAFILE 'D:\ARABANK_DB\CRA_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CRA_IDX DATAFILE 'D:\ARABANK_DB\CRA_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CRT_DBS DATAFILE 'D:\ARABANK_DB\CRT_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE CRT_IDX DATAFILE 'D:\ARABANK_DB\CRT_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE DEP_DBS DATAFILE 'D:\ARABANK_DB\DEP_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE DEP_IDX DATAFILE 'D:\ARABANK_DB\DEP_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE FST_DBS DATAFILE 'D:\ARABANK_DB\FST_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE FST_IDX DATAFILE 'D:\ARABANK_DB\FST_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE GDS_DBS DATAFILE 'D:\ARABANK_DB\GDS_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE GDS_IDX DATAFILE 'D:\ARABANK_DB\GDS_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE GNL_DBS DATAFILE 'D:\ARABANK_DB\GNL_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE GNL_IDX DATAFILE 'D:\ARABANK_DB\GNL_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE ILC_DBS DATAFILE 'D:\ARABANK_DB\ILC_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE ILC_IDX DATAFILE 'D:\ARABANK_DB\ILC_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE LGR_DBS DATAFILE 'D:\ARABANK_DB\LGR_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE LGR_IDX DATAFILE 'D:\ARABANK_DB\LGR_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE BIGFILE TABLESPACE LON_DBS DATAFILE 'D:\ARABANK_DB\LON_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE LON_IDX DATAFILE 'D:\ARABANK_DB\LON_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE LOT_DBS DATAFILE 'D:\ARABANK_DB\LOT_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE LOT_IDX DATAFILE 'D:\ARABANK_DB\LOT_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE SIG_DBS DATAFILE 'D:\ARABANK_DB\SIG_DBS.DAT' SIZE 10M AUTOEXTEND ON NEXT 20M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE SIG_IDX DATAFILE 'D:\ARABANK_DB\SIG_IDX.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE tlr_dbs DATAFILE 'D:\ARABANK_DB\tlr_dbs.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;
                        CREATE TABLESPACE tlr_idx DATAFILE 'D:\ARABANK_DB\tlr_idx.DAT' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE UNLIMITED LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE BLOCKSIZE 8K SEGMENT SPACE MANAGEMENT AUTO FLASHBACK ON;

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
