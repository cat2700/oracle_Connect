
import catClass
# from datetime import datetime
# import datetime
import threading
import time


cn = catClass.mainClass()

conf = cn.readConfig(configFileName="oracleConfig",
                     tags=['usrid', 'pass', 'sevice', 'ip'])

cn = catClass.mainClass(
    uid=f'{conf[0]}', upsw=f'{conf[1]}', service_name=f'{conf[2]}', ip=f'{conf[3]}')
if cn.open_connect():
    print('oracle connected')
else:
    print('error in connected')


# ==>  pyinstaller run.py --onefile --noconsole --debug=all
# ==> pip install cryptography
# conf = cn.readConfig(configFileName="shmolConfig",
#                      tags=['kind', 'fromOrcl', 'fromEx', 'filePathName', 'sheetName', 'colList', 'sql', 'maxRowsNum'])

# cn.convertToXML(kind=conf[0], fromOrcl=bool(int(conf[1])), fromEx=bool(int(conf[2])), filePathName=str(conf[3]),
#                 sheetName=int(conf[4]), colList=list(conf[5]), sql=str(conf[6]), maxRowsNum=int(conf[7]))


dat = '24/03/2022'
wherecond = ''

sql = f"""

        select a.zone_name,a.sector_name,a.sub_branch_no,a.sub_branch_name,
            nvl(curr.current_acc_counts,0) ,nvl(curr.current_acc_val,0) ,nvl(curr.current_acc_cus_count,0) ,
            nvl(sav.sav_Counts,0) ,nvl(sav.sav_val,0) ,nvl(sav.sav_cus_count,0) ,
            nvl(depost.Term_depost_counts,0) ,nvl(depost.Term_depost_val,0) ,nvl(depost.Term_cus_count,0) ,
            nvl(crt.crt_count,0) ,nvl(crt.crt_val,0) ,nvl(crt.crt_cus,0) ,
            nvl(lmaster.loan_approv_count,0) ,nvl(lmaster.loan_val,0) ,nvl(lmaster.loan_cus_count,0) ,
            nvl(db.current_acc_db_count,0) ,nvl(db.current_acc_db_val,0) ,nvl(db.current_acc_db_cus,0) ,
            nvl(cusDain.cusOfDain,0) ,
            nvl(cusMdin.cusOfMdin,0) ,
            nvl(cusAll.cusOfAll,0)
        from abe_banks_zone a
        left join
            (select branch_no, count(*) current_acc_counts ,sum(bal_blnc) current_acc_val , count(distinct(cus_no)) current_acc_cus_count
                    from bal_cr_tab where bal_acc_no between 500 and 599 and bal_close_dt is null and bal_blnc > 0
                    group by branch_no
            ) curr
        on to_char(a.sub_branch_no) = to_char(curr.branch_no)
        left join
            (select branch_no, count(*) sav_Counts ,sum(bal_blnc) sav_val ,count(distinct(cus_no)) sav_cus_count
                from bal_cr_tab where bal_acc_no between 700 and 720 and bal_close_dt is null and bal_blnc > 0
                    group by branch_no
        ) sav
        on to_char(a.sub_branch_no) = to_char(sav.branch_no)
        left join
        (select  branch_no,  count(*) Term_depost_counts ,sum(deposit_tab.dp_amount) Term_depost_val ,count(distinct(cus_no)) Term_cus_count
                    from deposit_tab where  deposit_tab.dp_upd_dsg not in (2,4,6) and deposit_tab.dp_val_dt > '{dat}'
                    group by branch_no
            ) depost
        on to_char(a.sub_branch_no) = to_char(depost.branch_no)
        left join
            (select branch_no, count(*) crt_count ,sum(certif_val) crt_val ,count(distinct(cus_no))  crt_cus
                        from crt_bal where  crt_bal.certif_flg in (1,2) and  crt_bal.to_dat > '{dat}'
                        group by branch_no
            ) crt
        on to_char(a.sub_branch_no) = to_char(crt.branch_no)
        left join
            (select branch_no, count(*) loan_approv_count ,sum(due_lon_amt) + sum(non_due_lon_amt) +
                                            sum(due_int) + sum(due_mgr_exp)+ sum(due_prot_exp) + sum(due_penlty) + sum(due_other) +
                                            sum(non_due_int) + sum(non_due_mgr_exp) + sum(non_due_prot_exp) + sum(non_due_other) loan_val
                                        ,count(distinct(cus_no)) loan_cus_count
                        from lon_master where  lon_master.loan_status not in (2,6,9)
                        group by branch_no
            ) lmaster
        on to_char(a.sub_branch_no) = to_char(lmaster.branch_no)
        left join
            (select branch_no, count(*) current_acc_db_count ,sum(bal_blnc) current_acc_db_val ,count(distinct(cus_no)) current_acc_db_cus
                        from bal_db_tab where bal_acc_no between 100 and 199 and bal_close_dt is null and bal_blnc > 0
                        group by branch_no
            ) db
        on to_char(a.sub_branch_no) = to_char(db.branch_no)
        left join
            (select substr(sss,1,9) branch_no, count(distinct(sss)) cusOfDain from
                    (
                                    select branch_no ||'-'|| cus_no as sss from bal_cr_tab b where b.bal_blnc >0 and b.bal_close_dt is null
                            union select branch_no ||'-'|| cus_no as sss from crt_bal c where c.certif_flg in (1,2) and c.to_dat >'{dat}'
                            union select branch_no ||'-'|| cus_no as sss from deposit_tab d where d.dp_upd_dsg not in (2,4,6) and d.dp_dlt_dt is null and d.dp_val_dt > '{dat}'
                    ) group by substr(sss,1,9)
                ) cusDain
        on to_char(a.sub_branch_no) = to_char(cusDain.branch_no)
        left join
                (select substr(nnn,1,9) branch_no, count(distinct(nnn)) cusOfMdin from
                    (
                                select branch_no ||'-'|| cus_no as nnn from LON_MASTER LS where LS.LOAN_STATUS not in (2,6,9)
                        union select branch_no ||'-'|| cus_no as nnn from bal_DB_tab DB where DB.bal_blnc >0 and DB.bal_close_dt is null
                    )group by substr(nnn,1,9)
                ) cusMdin
        on to_char(a.sub_branch_no) = to_char(cusMdin.branch_no)
        left join
                (select  substr(aaa,1,9) branch_no, count(distinct(aaa)) cusOfAll from
                    (
                                select branch_no ||'-'|| cus_no as aaa from bal_cr_tab b where b.bal_blnc >0 and b.bal_close_dt is null
                        union  select branch_no ||'-'|| cus_no as aaa from crt_bal c where c.certif_flg in (1,2) and c.to_dat >'{dat}'
                        union  select branch_no ||'-'|| cus_no as aaa from deposit_tab d where d.dp_upd_dsg not in (2,4,6) and d.dp_dlt_dt is null and d.dp_val_dt > '{dat}'
                        union  select branch_no ||'-'|| cus_no as aaa from LON_MASTER LS where LS.LOAN_STATUS not in (2,6,9)
                        union  select branch_no ||'-'|| cus_no as aaa from bal_DB_tab DB where DB.bal_blnc >0 and DB.bal_close_dt is null
                    )group by substr(aaa,1,9)
                ) cusAll
        on to_char(a.sub_branch_no) = to_char(cusAll.branch_no)

        {wherecond}

    """

# start_time = time.time()

# cn.exportOrclExc(sql=sql, excelName='temp.xlsx', sheetName='shmol')

# print(f"---{time.time() - start_time} seconds ---")

cn.encrOrDecr.