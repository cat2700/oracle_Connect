
import catClass
# from datetime import datetime
# import datetime
import threading
import time


cn = catClass.mainClass()

conf = cn.readConfig(configFileName="oracleConfig",
                     tags=['usrid', 'pass', 'sevice'])

cn = catClass.mainClass(
    uid=f'{conf[0]}', upsw=f'{conf[1]}', service_name=f'{conf[2]}', ip='172.29.107.44')
if cn.open_connect():
    print('oracle connected')
else:
    print('error in connected')


# print(cn.close_connect())
# sql = """
#     select           CUS_CIVIL_NO as nationalId,
#                      '' as secondaryId ,
#                      '' as secondaryIdType ,
#                      CUS_NAM_L as arabicName,
#                      '' as englishName ,
#                      CUS_BIRTHDAY as birthDate,
#                      birth_gov_cod as birthGovCode,
#                      CBE_GENDER as gender,
#                      id_gov_cod as residenceGovCode,
#                      CBE_NATIONAL_ALPHA  as nationality
#  from customer_tab_good_sh_1_22 sh,cbe_gender@abe_31102021,cbe_national@abe_31102021
#  where sh.cus_sex = cbe_gender.abe_gendera
#  and sh.cus_nationalt = cbe_national.cbe_national_code
#   and BRANCH_NO=910004000
# """
# rs = cn.runSQL(sql)
# rs = list(cn.runSQL("insert into TEMP values ('2','حامد محمد')"))
# L = [["1", "Fredico"], ["2", "haitham"]]
# print(type(L))
# rs = cn.runSQL("insert into TEMP values (:1, :2)", L)
# sql = f"delete from TEMP where cod ='1'"
# rs = cn.runSQL(sql)

# print(rs)
# print(type(rs[2]))

# for o in rs[2]:
#     print(o[:])

# cn.backupORRestore(
#     isrestore=True, restoreFile='all-16-02-2022')


# cn.convertToXML(fromEx=True, filePathName=r'',
#                 colList=['CUS_CIVIL_NO', 'a', 'b', 'CUS_NAM_L', 'c', 'CUS_BIRTHDAY',
#                          'BIRTH_GOV_COD', 'CBE_GENDER', 'ID_GOV_COD', 'CBE_NATIONAL_ALPHA'],
#                 maxRowsNum=0)


# cn.convertToXML(kind='acc', fromEx=True, filePathName=r'DEP all.xlsx',
#                 colList=['ACCOUNTID', 'TYPEID', 'CURRENCYID', 'BRANCHID', 'ISJOINT', 'OPENINGDATE',
#                          'NATIONALID', 'SECONDARYID', 'SECONDARYIDTYPE', 'CLOSINGDATE', 'STATUSID', 'STATUSREASON'],
#                 maxRowsNum=0)

# sql = """
#     select           CUS_CIVIL_NO as nationalId,
#                      '' as secondaryId ,
#                      '' as secondaryIdType ,
#                      CUS_NAM_L as arabicName,
#                      '' as englishName ,
#                      CUS_BIRTHDAY as birthDate,
#                      birth_gov_cod as birthGovCode,
#                      --CBE_GENDER as gender,
#                      id_gov_cod as residenceGovCode --,
#                      --CBE_NATIONAL_ALPHA  as nationality
#                      ,'' ,''
#  from customer_tab_good_sh_1_22 sh
#  --,cbe_gender@abe_31102021,cbe_national@abe_31102021
#  where
#  --sh.cus_sex = cbe_gender.abe_gendera
#  --and sh.cus_nationalt = cbe_national.cbe_national_code
#  -- and
#   BRANCH_NO=910004000 and id_gov_cod=0
# """

# sql = """
#     select           CUS_CIVIL_NO as nationalId,
#                      '' as secondaryId ,
#                      '' as secondaryIdType ,
#                      CUS_NAM_L as arabicName,
#                      '' as englishName ,
#                      CUS_BIRTHDAY as birthDate,
#                      birth_gov_cod as birthGovCode,
#                      id_gov_cod as residenceGovCode,
#                      '' ,''
#  from customer_tab_good_sh_1_22 sh
#    where  substr(branch_no,1,3) in 901
#    --and rownum <100000
#    --and birth_gov_cod between 14 and 14
#    --and id_gov_cod between 14 and 14
#    --and (extract(year from CUS_BIRTHDAY)  in (1948) )

# """
# 24803020102167


# rs = list(cn.runSQL(sql))
# print(rs)
# # return
# print(list(rs[2]))
# print(type(rs[2]))
# da = rs[2]
# print(da)
# n = rs[2][0][1]
# # nn = datetime.strptime(str(n), r"%Y-%m-%d %H:%M:%S").date()
# # print(nn)
# # print(rs[1])
# for indx, item in enumerate(da):
#     da[indx] = list(da[indx])
# print(da)

# nt = type(None)
# # dt = type(datetime.datetime)
# for o in da:
#     for indx, item in enumerate(o):
#         if type(item) is nt:
#             o[indx] = ''
#         elif isinstance(item, datetime.datetime):
#             y = datetime.datetime.strptime(
#                 str(item), r"%Y-%m-%d %H:%M:%S").year
#             m = datetime.datetime.strptime(
#                 str(item), r"%Y-%m-%d %H:%M:%S").month
#             d = datetime.datetime.strptime(str(item), r"%Y-%m-%d %H:%M:%S").day
#             o[indx] = str(y) + str(m).zfill(2) + str(d).zfill(2)

# print(da)
# cn.convertToXML(kind='cust', fromOrcl=True, sql=sql)

# ----------------
# def tst():

#     L = list(cn.readExcel(FPath='Book1.xlsx', colmnList=['code', 'name']))
#     # for r in L:
#     #     print(r)
#     print(cn.insertMany("insert into TEMP values (:1, :2)", L))
#     # for r in L:
#     #     print(list(r))
#     #     print(cn.runSQL("insert into TEMP values (:1, :2)", list(r)))
#     #


# tmr = 0.01
# x = 0
# while x < 2:
#     x += 1
#     print('start waiting')
#     t = threading.Timer(tmr, tst)
#     print('before starting')
#     t.start()
#     print('started')
#     t._wait_for_tstate_lock()
#     print('finished')


# print("=" * 9)
# # print('t is :' + str(t.is_alive()))


# cn.ReadWalletFiles('a')


# ================


# conf = cn.readConfig(configFileName="shmolConfig",
#                      tags=['kind', 'fromOrcl', 'fromEx', 'filePathName', 'sheetName', 'colList', 'sql', 'maxRowsNum'])
# if len(conf) == 8:
#     def run():
#         cn.convertToXML(kind=f'{conf[0]}', fromOrcl=bool(
#             int(conf[1])), fromEx=bool(int(conf[2])), filePathName=f'{conf[3]}', sheetName=int(conf[4]),
#             colList=list(conf[5]), sql=f'{conf[6]}', maxRowsNum=int(conf[7]))
#     t = threading.Timer(0.01, run)
#     t.start()
#     start_time = time.time()
#     print('started')
#     t._wait_for_tstate_lock()
#     print('finished')
#     print(f"---{time.time() - start_time} seconds ---")

# else:
#     print("should be get 8 args, chick shmolConfig File args")


# =======================================
# start_time = time.time()
# cn.procShmool(fetchAll=True)
# print(f"---{time.time() - start_time} seconds ---")

# start_time = time.time()
# cn.ReadWalletFiles('a')
# print(f"---{time.time() - start_time} seconds ---")

# start_time = time.time()
# sql = """
#     select * from MEEZA_CIF_XML
#     where rownum < 10
# """
# cn.convertToXML(kind='cust', fromOrcl=True, sql=sql, maxRowsNum=500000)
# print(f"---{time.time() - start_time} seconds ---")

# ==>  pyinstaller run.py --onefile --noconsole --debug=all
# conf = cn.readConfig(configFileName="shmolConfig",
#                      tags=['kind', 'fromOrcl', 'fromEx', 'filePathName', 'sheetName', 'colList', 'sql', 'maxRowsNum'])

# cn.convertToXML(kind=conf[0], fromOrcl=bool(int(conf[1])), fromEx=bool(int(conf[2])), filePathName=str(conf[3]),
#                 sheetName=int(conf[4]), colList=list(conf[5]), sql=str(conf[6]), maxRowsNum=int(conf[7]))


