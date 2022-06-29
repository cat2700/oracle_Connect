
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


dat = '23/06/2022'
wherecond = """
    where a.sub_branch_no in 
    (
        
    915008020,
    915008040,
    915008070,
    915008010,
    915008060,
    915008050,
    917004030,
    917004020,
    918002020,
    915010040,
    915010030,
    915010020,
    915010050,
    905002040,
    905002050,
    905008040,
    905002030,
    902006010,
    902006030,
    902006040,
    910007070,
    910007040,
    913002030,
    913002070,
    913002020,
    913002080,
    913002050,
    913002010,
    913002060,
    961002050,
    906001050,
    915004070,
    915004010,
    915004050,
    915004020,
    915004060,
    917008000,
    917002000,
    917002020,
    917002090,
    917002030,
    917002060,
    917002050,
    917002140,
    917002130,
    917002070,
    960003040,
    960003030,
    960003010,
    909004030,
    909004020,
    909004050,
    909004010,
    909004040,
    909003090,
    909003020,
    909003010,
    909003030,
    909003040,
    909003100,
    909003070,
    909003050,
    913006020,
    913006090,
    913006080,
    913006010,
    913006050,
    913006040,
    913006030,
    913006150,
    913006060,
    913006120,
    913006070,
    913006130,
    913006100,
    913006160,
    960002030,
    960002040,
    902002080,
    902002020,
    902002030,
    902002040,
    902002070,
    902002060,
    902002050,
    911003100,
    911003080,
    911003070,
    911003010,
    911003060,
    911003050,
    911003040,
    911003030,
    911003020,
    910014020,
    912004030,
    912004020,
    912004040,
    915009020,
    915009040,
    915009010,
    915009050,
    909011040,
    909011030,
    909011020,
    915011080,
    915011050,
    915011040,
    915011010,
    915011030,
    915011060,
    909001100,
    909001070,
    909001020,
    909001030,
    909001090,
    909001040,
    909001050,
    913008050,
    913008060,
    913008020,
    913008070,
    913008040,
    913008010,
    903007020,
    903007030,
    903007090,
    903007060,
    903007080,
    903007070,
    903007050,
    915007020,
    915007030,
    915007010,
    901001020,
    901001010,
    901001040,
    907006060,
    907006040,
    907006030,
    907006050,
    907006020,
    915001020,
    915001030,
    915001070,
    915001050,
    915001040,
    909002040,
    909002100,
    909002090,
    909002080,
    909002010,
    909002030,
    909002060,
    909002050,
    909002020,
    909002070,
    908002080,
    908002030,
    908002040,
    917001020,
    917001050,
    917001080,
    917001060,
    917001090,
    917001030,
    917001040,
    917001070,
    917001100,
    904008040,
    904008050,
    904008030,
    904008020,
    913001070,
    913001120,
    913001030,
    913001130,
    913001100,
    913001080,
    913001060,
    913001020,
    913001110,
    913001090,
    913007140,
    913007070,
    913007050,
    913007060,
    913007040,
    913007020,
    913007030,
    913007080,
    913007100,
    913007130,
    913007110,
    913007010,
    913007120,
    912002010,
    912002060,
    912002050,
    912002020,
    912002030,
    911004100,
    911004060,
    961002010,
    961003000,
    961002040,
    961001020,
    961002020,
    961002060,
    961001000,
    961001030,
    961001040,
    961002030,
    917002150,
    915001060,
    915004030,
    908001010


    )

        """

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

start_time = time.time()

cn.exportOrclExc(
    sql=sql, excelName=f'{dat.replace(r"/", "")}.xlsx', sheetName='shmol')

print(f"---{time.time() - start_time} seconds ---")
