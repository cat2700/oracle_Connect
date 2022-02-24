
import conn


cn = conn.my_conn(uid='arabank', upsw='icl', service_name="oracl2k")
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
# L = [("1", "Fredico"), ("2", "haitham")]
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


cn.convertToXML(kind='acc', fromEx=True, filePathName=r'*',
                colList=['ACCOUNTID', 'TYPEID', 'CURRENCYID', 'BRANCHID', 'ISJOINT', 'OPENINGDATE',
                         'NATIONALID', 'SECONDARYID', 'SECONDARYIDTYPE', 'CLOSINGDATE', 'STATUSID', 'STATUSREASON'],
                maxRowsNum=0)

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
# # cn.convertToXML(fromOrcl=True, sql=sql)
