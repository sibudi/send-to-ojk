from aliyunsdkkms.request.v20160120.DecryptRequest import DecryptRequest
from aliyunsdkkms.request.v20160120.EncryptRequest import EncryptRequest
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.auth import credentials

from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from tablestore import *

import base64
import csv
import errno
import json
import logging
import os
import pyminizip
import pymysql
import pysftp
import requests
import smtplib, ssl



logger = logging.getLogger()

#disable sftp Host Key checking
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

#yesterday = date.today() - timedelta(days=1)
yesterday = date.today() if (datetime.now()+timedelta(hours=7)).strftime('%Y%m%d') == (date.today()+timedelta(days=1)).strftime('%Y%m%d') else date.today() - timedelta(days=1)

def handler(event, context):
    try:
        environ = os.environ
        payload = json.loads(event)['payload']
        filename = '820053' + yesterday.strftime('%Y%m%d') + 'SIK01'
        filepath = environ['HOME'] + '/' +  filename

        apsara_config = get_configuration(environ, context, 'apsara')
        connection = connect_to_apsara(context, apsara_config)


        
        if payload == "SEND_TO_OJK":
            #delete
            with connection.cursor() as cursor:
                sql = """ delete from sik_fdc where tgl_pelaporan_data = subdate(current_date, 1) """

                cursor.execute(sql)

            connection.commit()


            #insert
            with connection.cursor() as cursor:
                sql = """
                            insert into sik_fdc
                            select null id
                                , 820053 id_penyelenggara
                                , b.uuid id_borrower
                                , 1 jenis_pengguna
                                , b.realName nama_borrower
                                , substring(b.idCardNo, 1, 16) no_identitas
                                , null no_npwp
                                , a.uuid id_pinjaman
                                , date(COALESCE(c.statusChangeTime, a.lendingTime)) tgl_perjanjian_borrower
                                , date(a.lendingTime) tgl_penyaluran_dana
                                , a.amountApply nilai_pendanaan
                                , subdate(current_date, 1) tgl_pelaporan_data
                                , CASE
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3))
                                        THEN 0
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8))
                                        THEN f.amountApply
                                    WHEN (a.status in (7,8) AND a.orderType = 0)
                                        THEN a.amountApply
                                    WHEN (a.status in (7,8) AND a.orderType = 3)
                                        #THEN (PERIOD_DIFF(DATE_FORMAT(d.refundTime, '%Y%m'), DATE_FORMAT(a.refundTime, '%Y%m')) + 1) * d.billAmout
                                        THEN IF((a.refundTime = d.refundTime), d.billAmout, IF((a.refundTime = d1.refundTime), d1.billAmout * 3, d2.billAmout *2)) #edit20200210
                                    ELSE 0 #jika order ext sudah lunas
                                END sisa_pinjaman_berjalan
                                , date(a.refundTime) tgl_jatuh_tempo_pinjaman
                                , CASE 
                                    WHEN (a.status in (7,8,10,11) AND datediff(COALESCE(a.actualRefundTime, subdate(current_date, 1)), date(a.refundTime)) < 30 AND a.orderType in (0,3)) 
                                        OR (a.status in (10,11) AND f.status in (7,8,10,11) AND datediff(COALESCE(f.actualRefundTime, subdate(current_date, 1)), date(f.refundTime)) < 30 AND a.orderType = 2)
                                        THEN 1
                                    WHEN (a.status in (10,11) AND (COALESCE(datediff(a.actualRefundTime, a.refundTime), 0) >= 30 AND COALESCE(datediff(a.actualRefundTime, a.refundTime), 0) <= 90) AND a.orderType in (0,3)) 
                                        OR (a.status in (7,8) AND (COALESCE(datediff(subdate(current_date, 1), date(a.refundTime)), 0) >= 30 AND COALESCE(datediff(subdate(current_date, 1), date(a.refundTime)), 0) <= 90) AND a.orderType in (0,3))
                                        OR (a.status in (10,11) AND f.status in (10,11) AND (datediff(f.actualRefundTime, f.refundTime) >= 30 AND datediff(f.actualRefundTime, f.refundTime) <= 90) AND a.orderType = 2)
                                        OR (a.status in (10,11) AND f.status in (7,8) AND (datediff(subdate(current_date, 1), date(f.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(f.refundTime)) <= 90) AND a.orderType = 2)
                                        THEN 2
                                    ELSE 3
                                END id_kualitas_pinjaman
                                , CASE
                                    WHEN (a.status in (7,8) AND a.orderType in (0,3))
                                        THEN greatest(COALESCE(datediff(subdate(current_date, 1), date(a.refundTime)), 0), 0)
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8)) 
                                        THEN greatest(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0), 0) #jika order ext blum lunas, hari keterlambatan terakhir = hari keterlambatan orderType 1
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3)) 
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, a.refundTime), 0), 0) #jika order normal/cicilan lunas, hari keterlambatan terakhir = actualRefundTime - refundTime
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11)) 
                                        THEN greatest(coalesce(datediff(f.actualRefundTime, f.refundTime), 0), 0) #jika order ext lunas, hari keterlambatan terakhir = actualRefundTime - refundTime yg kedua
                                    ELSE 0
                                END status_pinjaman_dpd
                                , CASE
                                    WHEN (a.status in (7,8) AND a.orderType = 0)
                                        THEN greatest(COALESCE(datediff(subdate(current_date, 1), date(a.refundTime)), 0), 0)
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8)) 
                                        THEN IF(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0) < datediff(a.actualRefundTime, a.refundTime), 
                                            greatest(COALESCE(datediff(a.actualRefundTime, a.refundTime), 0), 0), 
                                            greatest(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0), 0)
                                        )
                                    WHEN (a.status in (10,11) AND a.orderType = 0) 
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, a.refundTime), 0), 0)
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11)) 
                                        THEN IF(greatest(coalesce(datediff(f.actualRefundTime, f.refundTime), 0), 0) < datediff(a.actualRefundTime, a.refundTime), 
                                            greatest(COALESCE(datediff(a.actualRefundTime, a.refundTime), 0), 0), 
                                            greatest(COALESCE(datediff(f.actualRefundTime, f.refundTime), 0), 0)
                                        )
                                    WHEN (a.status in (7,8) AND a.orderType = 3)
                                        THEN IF(d1.actualRefundTime is NULL, 
                                            greatest(COALESCE(datediff(subdate(current_date, 1), date(d1.refundTime)), 0), 0), 
                                            IF(d2.actualRefundTime is NULL, 
                                                greatest(coalesce(datediff(subdate(current_date, 1), date(d2.refundTime)), 0), coalesce(datediff(date(d1.actualRefundTime), date(d1.refundTime)), 0), 0), #edit20200210
                                                greatest(coalesce(datediff(subdate(current_date, 1), date(d.refundTime)), 0), coalesce(datediff(date(d1.actualRefundTime), date(d1.refundTime)), 0), coalesce(datediff(date(d2.actualRefundTime), date(d2.refundTime)), 0), 0) #edit20200210
                                            )
                                        )
                                    WHEN (a.status in (10,11) AND a.orderType = 3)
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, d.refundTime), 0), coalesce(datediff(d2.actualRefundTime, d2.refundTime), 0), coalesce(datediff(d1.actualRefundTime, d1.refundTime), 0), 0)
                                    ELSE 0
                                END status_pinjaman_max_dpd
                                , CASE 
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3)) 
                                        OR (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11))
                                            THEN 'L' #kalo nasabah bayar di hari yg sama query dijalankan, dia tidak akan terambil, krn pake filter date(a.updateTime) < CURRENT_DATE
                                            #tapi harusnya dia tetap terambil dengan status O (kalo status W aman krn tdk diambil ulang)
                                    WHEN (a.status in (7,8) AND a.orderType = 0 AND datediff(subdate(current_date, 1), date(a.refundTime)) <= 90) 
                                        OR (a.status in (7,8) AND a.orderType = 3 AND datediff(subdate(current_date, 1), date(d.refundTime)) <= 90)
                                        OR (a.status in (10,11) AND a.orderType = 2 AND datediff(subdate(current_date, 1), date(f.refundTime)) <= 90 AND f.status in (7,8))
                                            THEN 'O'
                                    ELSE 'W'
                                END status_pinjaman
                                , date(a.updateTime)
                            from ordOrder a
                            left join usrUser b
                                on a.userUuid = b.uuid
                            left join ordHistory c
                                on a.uuid = c.orderId
                                and c.status = 20
                                and c.updateTime = (select max(c2.updateTime) from ordHistory c2 where c2.orderId = c.orderId and c2.status = 20)
                            left join ordBill d
                                on a.uuid = d.orderNo
                                and a.orderType = 3
                                and d.billTerm = 3
                            left join ordBill d1
                                on a.uuid = d1.orderNo
                                and a.orderType = 3
                                and d1.billTerm = 1
                            left join ordBill d2
                                on a.uuid = d2.orderNo
                                and a.orderType = 3
                                and d2.billTerm = 2
                            left join ordDelayRecord e
                                on a.uuid = e.orderNo
                            left join ordOrder f
                                on e.delayOrderNo = f.uuid
                            where 1
                            and a.disabled = 0
                            and b.disabled = 0
                            and a.status in (7, 8, 10, 11)
                            and a.orderType in (0, 2, 3)
                            and (
                                (
                                date(a.updateTime) = subdate(CURRENT_DATE,1) and a.orderType <> 3)
                                or (datediff(subdate(current_date, 1), date(a.refundTime)) <= 91 and a.status in (7,8))
                                or (datediff(subdate(current_date, 1), date(f.refundTime)) <= 91 and f.status in (7,8))
                                or (a.orderType = 3 and a.status in (10,11) and date(a.actualRefundTime) = subdate(current_date, 1))
                            )
                """
                cursor.execute(sql)

            connection.commit()


            #read
            with connection.cursor() as cursor:
                sql = """
                        SELECT id_penyelenggara,
                            id_borrower,
                            jenis_pengguna,
                            nama_borrower,
                            no_identitas,
                            no_npwp,
                            id_pinjaman,
                            date_format(tgl_perjanjian_borrower, '%Y%m%d') tgl_perjanjian_borrower,
                            date_format(tgl_penyaluran_dana, '%Y%m%d') tgl_penyaluran_dana,
                            nilai_pendanaan,
                            date_format(tgl_pelaporan_data, '%Y%m%d') tgl_pelaporan_data,
                            sisa_pinjaman_berjalan,
                            date_format(tgl_jatuh_tempo_pinjaman, '%Y%m%d') tgl_jatuh_tempo_pinjaman,
                            id_kualitas_pinjaman,
                            status_pinjaman_dpd,
                            status_pinjaman_max_dpd,
                            status_pinjaman
                        FROM sik_fdc 
                        where tgl_pelaporan_data = subdate(current_date,1)
                """
                cursor.execute(sql)
                data = cursor.fetchall()
                csv_column_order = list(data[0].keys())


            with open(filepath + '.csv', mode='w', newline='') as f:
                writer = csv.DictWriter(f, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=csv_column_order)
                for row in data:
                    writer.writerow(row)


            #zipfile
            pyminizip.compress(filepath + '.csv', None, filepath + '.zip' , 'HbWSJjvMY7#2', 0)

            #put file to sftp
            sftp_put(filepath + '.zip')

            #delete file csv
            silentremove(filepath + '.csv')
            #delete file zip
            silentremove(filepath + '.zip')


        elif payload == "CHECK_OJK_RESULT":
            #get file from sftp
            sftp_get(filename + '.zip.out', filepath + '.zip.out')

            #read file
            with open(filepath + '.zip.out', 'rb') as reader:
                file = base64.b64encode(reader.read())

            #read db for construct message
            with connection.cursor() as cursor:
                sql = """
                       select tgl_pelaporan_data, count(*) count
                       from sik_fdc 
                       where tgl_pelaporan_data >= subdate(current_date, 7) and tgl_pelaporan_data <= subdate(current_date,1) 
                       group by tgl_pelaporan_data 
                       order by 1
                """
                cursor.execute(sql)
                data = cursor.fetchall()

            message = f"Terlampir hasil output dari folder out sftp pusdafil.<br /><br />"
            message += f"<table style='width: 30%;' border='2' cellpadding='1'>"
            message += f"<tr><th>tanggal</th> <th>count</th></tr>"
            for row in data: #print as row
                message += f"<tr><td>{row['tgl_pelaporan_data']}</td> <td>{row['count']}</td></tr>"
            message += f"</table>"


            #send notification
            send_notification(filename, file, message, environ, context)

            #delete file zip.out
            silentremove(filepath + '.zip.out')

    except Exception as e:
        logger.error(e)



def get_configuration(environ, context, group_code):
    creds = context.credentials
    client = OTSClient(environ['TABLE_STORE_ENDPOINT'], 
        creds.accessKeyId, creds.accessKeySecret, environ['TABLE_STORE_INSTANCE_NAME'], sts_token=creds.securityToken)
    primary_key = [('group', group_code)]
    columns_to_get = []
    consumed, return_row, next_token = client.get_row(environ['TABLE_STORE_TABLE_NAME'], primary_key, columns_to_get, None,1)
    json = {}
    for att in return_row.attribute_columns:
        json[att[0]]=att[1]
    return json


def connect_to_apsara(context, apsara_config):
    return pymysql.connect(
        host=apsara_config['host'],
        user=apsara_config['username'],
        password=decrypt_string(context, apsara_config['password']),
        db=apsara_config['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def decrypt_string(context, encrypted_string):
    creds = context.credentials
    sts_credentials = credentials.StsTokenCredential(creds.accessKeyId, creds.accessKeySecret, creds.securityToken) 
    client = AcsClient(
        region_id = 'ap-southeast-5',
        credential = sts_credentials
    )
    request = DecryptRequest()
    request.set_CiphertextBlob(encrypted_string)
    response = str(client.do_action_with_exception(request), encoding='utf-8')
    return json.loads(response)['Plaintext']


def silentremove(filename):
    try:
        os.remove(filename)
        logger.info("sukses delete " + filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred


def sftp_put(filepath):
    with pysftp.Connection('pusdafil.afpi.or.id', username='820053', password='Ht7VyhHMa7Akyetk', cnopts=cnopts) as sftp:
        with sftp.cd('in'):
            sftp.put(filepath)


def sftp_get(filename, filepath):
    with pysftp.Connection('pusdafil.afpi.or.id', username='820053', password='Ht7VyhHMa7Akyetk', cnopts=cnopts) as sftp:
        with sftp.cd('out'):
            sftp.get(filename, filepath, preserve_mtime=True)


def send_notification(filename, file, message, environ, context):
    filename = '"filename":' + '"' + filename + '.zip.out"'
    file = '"content":' + '"' + file.decode('utf-8') + '"'
    attachments = '{' + filename + ', ' + file + '}'
    data = {
                "message": message,
                "subject": "SIK FDC Notification " + yesterday.strftime('%Y%m%d'),
                "cc": "setiya.budi@do-it.id",
                "to": "setiya.budi@do-it.id",
                "attachments": [ json.loads(attachments) ]
            }
    notification_config = get_configuration(environ, context, 'notification') 
    token = notification_config['x-authorization-token']
    headers = {'Content-Type': 'application/json', 'x-authorization-token': token}
    resp = requests.post('https://notificationapi.doitglotech.co.id/email', data=json.dumps(data), headers=headers)