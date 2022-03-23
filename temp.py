from datetime import date as dt
import datetime as dati
import math

row = ['$%^&',	910015000,	10908, '28201012106352', '19520112', '21', '10', '2']
error, good, civils = [], [], []


def process(row):
    try:
        # slicing row
        row_rowID, row_br, row_cusNum, row_civil, row_birth, row_gov, row_rais, row_sex = \
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
        # catch civil_no
        row_civilLength = len(row_civil)
        karn = row_civil[:1]
        if karn == '2':
            K = '19'
        elif karn == '3':
            K = '20'
        else:
            k = '99'
        yy = row_civil[1:3]
        mm = row_civil[3:5]
        dd = row_civil[5:7]
        try:
            civilBirthDate = dt(
                year=int(str(K) + str(yy).zfill(2)), month=int(str(mm).zfill(2)), day=int(str(dd).zfill(2)))
        except Exception as er:
            civilBirthDate = None
        civil_sx = row_civil[12:13]
        civil_gov = row_civil[7:9]
        # catch birth day
        try:
            BirthDate = dt(
                year=int(row_birth[:4]), month=int(row_birth[4:6]), day=int(row_birth[6:8]))
        except Exception as er:
            BirthDate = None
        # catch gov code
        gov = str(row_gov).zfill(2)
        ris_gov = str(row_rais).zfill(2)
        gov_tbl = ['01', '02', '03', '04', '05', '06', '11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '23', '24', '25', '26', '27', '28', '29', '31', '32', '33', '34', '35', '88'
                   ]
        # ==> chk digit
        try:
            first_round = int(row_civil[0:1]) * 2 + int(row_civil[1:2]) * \
                7 + int(row_civil[2:3]) * 6 + int(row_civil[3:4]) * \
                5 + int(row_civil[4:5]) * 4 + int(row_civil[5:6]) * \
                3 + int(row_civil[6:7]) * 2 + int(row_civil[7:8]) * \
                7 + int(row_civil[8:9]) * 6 + int(row_civil[9:10]) * \
                5 + int(row_civil[10:11]) * 4 + int(row_civil[11:12]) * \
                3 + int(row_civil[12:13]) * 2
            current_chk_dig = int(row_civil[13:14])
            chk_dig_civ = int(row_civil[13:14])
            second_round = math.trunc(first_round/11)
            third_round = second_round * 11 - first_round

            if third_round == 0:
                chk_dig = 1
            else:
                chk_dig = 11 + third_round
        except Exception as er:
            chk_dig = None

    except Exception as er:
        print('error in catching')

    # print(karn, yy, mm, dd, civilBirthDate, BirthDate, type(
    #     civilBirthDate), civilBirthDate.year, civil_gov, gov)
    # print("*" * 100)

    # chk for 14 digit
    if row_civilLength != 14:
        error.append(('1', 'رقم بالرقم القومي - لا يساوي 14',
                      row_rowID, row_br, row_cusNum))
        return
    elif karn not in ('2', '3'):
        error.append(('2', 'خطا بالرقم القومي - رقم القرن',
                      row_rowID, row_br, row_cusNum))
        return
    elif civilBirthDate == None or BirthDate == None:
        error.append(('3', 'تاريخ الميلاد غير صحيح',
                      row_rowID, row_br, row_cusNum))
        return
    elif civilBirthDate != BirthDate:
        error.append(('4', 'تاريخ الميلاد المسجل لا يتطابق مع الرقم القومي',
                      row_rowID, row_br, row_cusNum))
        return
    elif civilBirthDate.year > dt.today().year or (karn == '2' and civilBirthDate.year < 1920):
        error.append(('5', 'خطا بالرقم القومي - سنة الميلاد',
                      row_rowID, row_br, row_cusNum))
        return
    # التأكد من رقم المحافظة
    elif civil_gov != gov:
        error.append(('6', 'رقم محافظة الميلاد غير متطابق بالرقم القومي',
                      row_rowID, row_br, row_cusNum))
        return
    elif civil_gov not in gov_tbl:
        error.append(('7', 'رقم محافظة الميلاد غير موجود بجدول المحافظات',
                      row_rowID, row_br, row_cusNum))
        return
    elif ris_gov not in gov_tbl:
        error.append(('8', 'رقم محافظة السكن غير موجود بجدول المحافظات',
                      row_rowID, row_br, row_cusNum))
        return
    # chick civile digit
    elif chk_dig == None:
        error.append(('9', 'رقم قومى يحتوى على حروف خاصة',
                      row_rowID, row_br, row_cusNum))
        return
    elif chk_dig != chk_dig_civ:
        error.append(('10', 'رقم قومى غير صحيح م',
                      row_rowID, row_br, row_cusNum))
        return
    # sex
    elif row_sex == '' or row_sex not in ('1', '2'):
        error.append(('', '',
                      row_rowID, row_br, row_cusNum))
        return
    elif (int(row_sex) % 2 == 0 and int(civil_sx) != 2) or (int(row_sex) % 2 != 0 and int(civil_sx) != 1):
        error.append(('', '',
                      row_rowID, row_br, row_cusNum))
        return
    elif row_civil in civils:
        error.append(('13', 'رقم قومى مكرر',
                      row_rowID, row_br, row_cusNum))
        return
    else:
        good.append((row_rowID, row_br, row_cusNum))
        civils.append(row_civil)


process(row=row)
print(error)
print(good)
print(civils)
