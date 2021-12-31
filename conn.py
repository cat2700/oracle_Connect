class my_conn:
    import cx_Oracle

    def __init__(self, uid, upsw, service_name, ip='127,0,0,1', port='1521', auto_dsn=False, dns_name=''):
        self.user = uid
        self.passw = upsw
        self.service = service_name
        self.ip = ip
        self.port = port
        self.auto_dsn = auto_dsn
        self.dns_name = dns_name

    def connect(selfe):

        connection = cx_Oracle.connect(user="arabank", password="icl",
                                       dsn="localhost/oracl2k")
        dsn = cx_Oracle.makedsn("172.29.107.44", 1521, service_name="oracl2k")
        connection = cx_Oracle.connect(user="abe_30112021", password='icl', dsn=dsn,
                                       encoding="UTF-8")
        connection = cx_Oracle.connect(user="abe_30112021", password='icl', dsn="DataServer-44",
                                       encoding="UTF-8")
        connection.open()


cursor = connection.cursor()
# cursor.execute("""
#         SELECT first_name, last_name
#         FROM employees
#         WHERE department_id = :did AND employee_id > :eid""",
#         did = 50,
#         eid = 190)
sql = 'select b.branch_no,b.bal_acc_no from arabank.bal_cr_tab b where b.cus_no=64328   and b.branch_no=919002000'
cursor.execute(sql)
for fname, lname in cursor:
    print("Values:", fname, lname)
connection.close()
