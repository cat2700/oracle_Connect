from datetime import date as dt
import datetime as dati
row = ['$%^&',	910015000,	10908, '25201121100762', '19520112',
       'ثريا مصطفى مصطفى غنيم',
       '33 ش 146 المشروع الامريكى ب 10 ق 6 حلوان القاهرة',
       '23 ش 146-المشروع الامريكى-ب10ق 6-حلولن -القاهرة',
       1,	2,	99999999,	19005,
       '', 1144075170,	1,	11,
       ]
err = []
good = []


def process(row):
    # slicing
    karn = row[3][:1]
    if karn == '2':
        K = '19'
    elif karn == '3':
        K = '20'
    else:
        k = '99'
    yy = row[3][1:3]
    mm = row[3][3:5]
    dd = row[3][5:7]
    try:
        # conBDate = str(K) + str(yy).zfill(2) + \
        #     str(mm).zfill(2) + str(dd).zfill(2)
        civilBirthDate = dt(
            year=int(str(K) + str(yy).zfill(2)), month=int(str(mm).zfill(2)), day=int(str(dd).zfill(2)))
    except Exception as err:
        civilBirthDate = None
    try:
        BirthDate = dt(
            year=int(row[4][:4]), month=int(row[4][4:6]), day=int(row[4][6:8]))
    except Exception as err:
        BirthDate = None
    sx = row[3][12:13]
    gov = row[3][7:9]
    print(karn, yy, mm, dd, sx, gov, civilBirthDate,
          BirthDate, type(civilBirthDate))

    # chk for 14 digit
    if len(row[3]) != 14:
        err.append(('1', 'رقم بالرقم القومي - لا يساوي 14',
                    row[0], row[1], row[2]))
        return
    elif karn not in ('2', '3'):
        err.append(('2', 'خطا بالرقم القومي - رقم القرن',
                    row[0], row[1], row[2]))
        return
    # elif civilBirthDate == None or BirthDate == None:
    #     err.append(('3', 'تاريخ الميلاد غير صحيح',
    #                 row[0], row[1], row[2]))
    #     return
    # elif civilBirthDate != BirthDate:
    #     err.append(('4', 'تاريخ الميلاد المسجل لا يتطابق مع الرقم القومي',
    #                 row[0], row[1], row[2]))
    #     return
    # elif dt(civilBirthDate).year > dt.today().year or (karn == '2' and dt(civilBirthDate).year < 1920):
    #     err.append(('5', 'خطا بالرقم القومي - سنة الميلاد',
    #                 row[0], row[1], row[2]))
    #     return
    elif 1 == 2:
        err.append(('6', 'خطا بالرقم القومي - شهر الميلاد',
                    row[0], row[1], row[2]))
        return
    else:
        good.append((row[0], row[1], row[2]))


process(row=row)
print(err)
print(good)
