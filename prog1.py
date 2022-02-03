import conn 
cn = conn.my_conn('arabank', 'icl', 'oracl2k')
print(cn.open_connect())
print(cn.close_connect())