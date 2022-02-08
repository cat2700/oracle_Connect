import cx_Oracle


class my_conn:

    connection = None

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
                cursor = self.connection.cursor()
                cursor.execute(SQLst)
                for accountid in cursor:
                    print("Values:", accountid)
                return True
        except BaseException as err:
            return False, err


cn = my_conn(uid='arabank', upsw='icl', saved_dns_name="oracl2k")
print(cn.open_connect())
# print(cn.close_connect())
print(cn.runSQL("select accountid from temp_dep"))

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
