import conn


cn = conn.my_conn(uid='arabank', upsw='icl', service_name="oracl2k")
print(cn.open_connect())
# print(cn.close_connect())
# rs = list(cn.runSQL("select * from temp_dep"))
# rs = list(cn.runSQL("insert into TEMP values ('2','حامد محمد')"))
# L = [("1", "Fredico"), ("2", "haitham")]
# print(type(L))
# rs = cn.runSQL("insert into TEMP values (:1, :2)", L)
sql = f"delete from TEMP where cod ='1'"
rs = cn.runSQL(sql)

print(rs)
# print(rs[1])
# for o in rs[2]:
#     print(o[:2])

cn.backUPme()
