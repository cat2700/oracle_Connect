
import catClass
from datetime import datetime
# import datetime
import threading
import time
import math


start_time = time.time()


cn = catClass.mainClass()

conf = cn.readConfig(configFileName="oracleConfig",
                     tags=['usrid', 'pass', 'sevice', 'ip'])

cn = catClass.mainClass(
    uid=f'{conf[0]}', upsw=f'{conf[1]}', service_name=f'{conf[2]}', ip=f'{conf[3]}')
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
# print(datetime.today().hour,datetime.today().minute,datetime.today().second)
# cn.procShmool()
# print(f"---{time.time() - start_time} seconds ---")

# start_time = time.time()
# cn.ReadWalletFiles('a')
# print(f"---{time.time() - start_time} seconds ---")


# sql = """
#     SELECT * FROM MEEZA_CARDS_XML WHERE BRANCH_CCH NOT IN (
#         6023,6513,6539,6263,6372,6858,6428,7041,6892,7057,6236,6248,6990,6694,6717,6864,6697,
#         6379,6776,6349,6194,6969,6950,7080,6273,6068,6351,6311,6456,6412,7111,6979,7034,7098,
#         6381,6203,6929,6871,7101,7134,6877,6195,6088,6438,6808,6792,6442,6327,6099,6854,7071,
#         7052,6882,6079,6688,6101,6340,6970,6961,6837,6820,7073,7064,7007,7119,6483,6323,6253,
#         7010,6842,6865,6853,6939,6488,7115,6422,6471,6714,6725,6698,6708
#     )
# """
# cn.convertToXML(kind='card', fromOrcl=True, sql=sql, maxRowsNum=500000)


# sss = """
#   select count(*)
#   from  (select * from customer_tab union all select * from customer_tab@islamic_31032022) c

# where  c.cus_kind = 0 and cus_opn_dat <= to_date('31/03/2022') and branch_no not in( 905001080,919009000)
# and   (branch_no,cus_no) not in (

#     select branch_no , cus_no from bal_cr_tab b
#     where --bal_blnc >0 and
#     bal_close_dt is null

#     union

#     select branch_no , cus_no from bal_cr_tab@islamic_31032022 b
#     where --bal_blnc >0 and
#     bal_close_dt is null

#     union

#     select  branch_no , cus_no from crt_bal c
#     where certif_flg in (1,2) and to_dat > '31/03/2022'

#     union

#     select  branch_no , cus_no from crt_bal@islamic_31032022 c
#     where certif_flg in (1,2)  and to_dat > '31/03/2022'

#     union

#     select  branch_no , cus_no from deposit_tab d
#     where dp_upd_dsg not in (2,4,6) and dp_dlt_dt is null and dp_val_dt > '31/03/2022'

#     union

#     select  branch_no , cus_no from deposit_s_tab@islamic_31032022 d
#     where dp_upd_dsg not in (2,4,6) and dp_dlt_dt is null and dp_val_dt > '31/03/2022'

#     union
#     select  branch_no , cus_no from LON_MASTER LS
#     where  LOAN_STATUS not in (2,6,9)

#     union
#     select branch_no , cus_no from bal_DB_tab DB
#     where  --bal_blnc >0 and
#     bal_close_dt is null

#     union
#     select branch_no , cus_no from bal_DB_tab@islamic_31032022 DB
#     where  --bal_blnc >0 and
#     bal_close_dt is null
#     )
#     --and c.cus_civil_no not in (select cus_civil_no from CUSTOMER_TAB_shmol_rej)
#     and c.cus_nationalt = 1
#     and substr(c.cus_civil_no,8,2) in (select lpad(g.gov_id,2,0) from gov_tab g)
#     and c.id_gov_cod in (select lpad(g.gov_id,2,0) from gov_tab g)
#     and substr(c.cus_civil_no,8,2) = to_char(lpad(c.birth_gov_cod,2,0))
#     and substr(c.cus_civil_no,2,6) = to_char(c.cus_birthday,'YYMMDD')

# """
# result = cn.runSQL(sss)
# print(result[0])
# print(result[2])


# cn.report5040(5, 2022)

print(cn.open_connect())


sql = """
    select t.companyuniqueid, t.companyname, t.economicsectorisicl4, a4.act_nam_l, t.db_flag from cbe_company_cif_view_db t 
    left join activ4_tab@shmool a4 on t.economicsectorisicl4 = a4.act_code4
    where t.companyuniqueid is not null and t.companyname is not null
    --and t.db_flag = 'not debit'

"""
cn.convertToXML(kind='company_cif', fromOrcl=True, sql=sql, maxRowsNum=500000)


print(f"---{time.time() - start_time} seconds ---")
