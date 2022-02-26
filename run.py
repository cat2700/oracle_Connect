
import catClass
# from datetime import datetime
# import datetime
import threading


cn = catClass.mainClass(uid='arabank', upsw='icl', service_name="oracl2k")
# print(cn.open_connect())
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


cn.ReadWalletFiles(FolderPath=r"C:\Users\admin\Desktop\oracle_Connect\walletA",
                   columnsHeader=['code', 'name'], sqlST=r'insert into TEMP values (:1, :2)')
