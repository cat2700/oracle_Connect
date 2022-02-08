# import conn 
import cx_Oracle

connection = None
ipp = '127.0.0.1' + '/' + 'oracl2k'
connection = cx_Oracle.connect(user='arabank', password='icl' ,dsn=ipp)
# print(connection.)