import os
import uuid
import re
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
from pytz import timezone, utc
import pandas as pd

columns_list = ['watch_number']
azure_portal_upload_list = pd.DataFrame(columns=columns_list)

'''
필요한 기능
1) 어떤 폴더 생성 되었는지 경로랑 같이 알려주기
2) 지정한 폴더만 다운 받도록 argparse
3_1) .3gp 뒤에 문자열 붙어 있으면 문자열 제거 하는 rename 작업 & rename 지정
3_2) renamed 된 파일 이동시키고(local Download 공간에서)
3_3) local Download 된 상태에서 동일한 형상으로 Blob 컨테이너에 업로드 하기


'''
new_watch_list = []
watch_sn_list = []


def blob_storage_connect(container_name):
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # Instantiate a ContainerClient
    container_client = blob_service_client.get_container_client(container_name)
    blobs_filename_list = container_client.list_blobs()
    return blobs_filename_list


def test(container_name, selected_watch):
    blobs_filename_list = blob_storage_connect(container_name)
    for blob_filename in blobs_filename_list:
        # 새로운 행 초기화
        if re.match(selected_watch, blob_filename.name):
            print(blob_filename.name, "success")
        else:
            print('error', blob_filename.name)
            pass


def cut_str(s, l):
    return [int(s[i:i+l]) for i in range(0, len(s), l)]


def strdate_to_datetime(str_date, str_time):
    _date = cut_str(str_date, 2)
    _date[0] += 2000
    _time = cut_str(str_time, 2)
    return datetime(*_date, *_time)


try:

    '''스토리지 계정 가운데, 사용할 storage 계정의 연결 문자열 저장 '''
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Instantiate a ContainerClient
    container_client = blob_service_client.get_container_client(
        "smartwatchdata")

    blobs_filename_list = container_client.list_blobs()
    lst_blobs_filename_list = list(blobs_filename_list)
    for blob_filename in blobs_filename_list:
        # 새로운 행 초기화
        if re.match('2020-11-24/20IHPA', blob_filename.name):
            # watch_sn 중복 제거
            print(blob_filename.name, "success")
            watch_sn = blob_filename.name.split('/')[-1].split('_')[0]
            record_day_null_timezone = blob_filename.name.split(
                '/')[-1].split('_')[1]
            record_time_null_timezone = blob_filename.name.split(
                '/')[-1].split('_')[2]
            # str 타입을 datetime으로 변경

            watch_datetime_datetime_null = strdate_to_datetime(
                record_day_null_timezone, record_time_null_timezone)
            # pytz.all_timezones ---- timezone 적용할 수 있는 목록 볼 수 있음
            # http://abh0518.net/tok/?p=635 ---------- timestamp 같이 활용하는 방안

            # watch 가 중국시각대일때
            record_watch_china = timezone('Asia/Shanghai')

            # watch가 중국 시각대이라고 가정하고 계산
            record_watch_datetime_null = watch_datetime_datetime_null.astimezone(
                record_watch_china)
            str_record_watch_datetime = str(record_watch_datetime_null)

            # beacon 정보
            beacon_mac_addr_number_4 = blob_filename.name.split(
                '/')[-1].split('_')[-1].split('.')[0][8:12]

            # KST 시간대 설정
            uploaded_Time_UTC = blob_filename.last_modified
            KST = timezone('Asia/Seoul')
            uploaded_datetime_KST = uploaded_Time_UTC.astimezone(KST)
            str_uploaded_datetime_KST = str(uploaded_datetime_KST)
            upload_day_KST = str_uploaded_datetime_KST.split(' ')[0]
            upload_time_KST = str_uploaded_datetime_KST.split(
                ' ')[-1].split('+')[0]
            # China 시간대 설정
            China = timezone('Asia/Shanghai')
            uploaded_datetime_China = uploaded_Time_UTC.astimezone(China)
            str_uploaded_datetime_China = str(uploaded_datetime_China)
            upload_day_China = str_uploaded_datetime_China.split(' ')[0]
            upload_time_China = str_uploaded_datetime_China.split(
                ' ')[-1].split('+')[0]

            test_diff_time_calculation = record_watch_datetime_null - uploaded_datetime_China
            str_test_diff_time_calculation = str(test_diff_time_calculation)
            diff_time_calculation = uploaded_datetime_China - record_watch_datetime_null
            str_diff_time_calcutaion = str(diff_time_calculation)

            # https://www.kite.com/python/answers/how-to-convert-a-timedelta-to-days,-hours,-and-minutes-in-python

            if not beacon_mac_addr_number_4:
                beacon_mac_addr_number_4 = 'null'
            else:
                pass

            azure_portal_upload_list = azure_portal_upload_list.append(
                {'watch_number': watch_sn,
                 'str_record_watch_datetime': str_record_watch_datetime,
                 'record_day_null_timezone': record_day_null_timezone,
                 'record_time_null_timezone': record_time_null_timezone,
                 'beacon_mac_addr_number_4': beacon_mac_addr_number_4,
                 'str_uploaded_datetime_KST': str_uploaded_datetime_KST,
                 'upload_day_KST': upload_day_KST,
                 'upload_time_KST': upload_time_KST,
                 'str_uploaded_datetime_China': str_uploaded_datetime_China,
                 'upload_day_China': upload_day_China,
                 'upload_time_China': upload_time_China,
                 'str_diff_time_calcutaion': str_diff_time_calcutaion, },
                ignore_index=True)

            azure_portal_upload_list.to_excel(
                "1124_upload_check.xlsx", encoding='utf-8')

            '''
            업로드 할 때만 사용하고 지금은 주석
            blob = BlobClient.from_connection_string(
                conn_str=connect_str, container_name="smartwatchdata", blob_name="{}".format(blob_filename.name))
            try:
                if blob_filename.name.count('/') == 0:
                    pass
                elif blob_filename.name.count('/') == 1:
                    os.makedirs(blob_filename.name.split('/')[0])
                    print("Depth_1 폴더 생성")
                elif blob_filename.name.count('/') == 2:
                    os.makedirs(blob_filename.name.split(
                        '/')[-3]+'\\'+blob_filename.name.split('/')[-2])
                    print("Depth_2 폴더 생성")
                elif blob_filename.name.count('/') == 3:
                    os.makedirs(blob_filename.name.split(
                        '/')[-4]+'\\'+blob_filename.name.split('/')[-3]+'\\'+blob_filename.name.split('/')[-2])
                    print("Depth_3 폴더 생성")
                elif blob_filename.name.count('/') == 4:
                    os.makedirs(blob_filename.name.split('/')[-5]+'\\'+blob_filename.name.split(
                        '/')[-4]+'\\'+blob_filename.name.split('/')[-3]+'\\'+blob_filename.name.split('/')[-2])
                    print("Depth_4 폴더 생성")

            except:
                pass
            with open("./{}".format(blob_filename.name), "wb") as my_blob:
                # with open("E://{}".format(blob_filename.name), "wb") as my_blob:
                blob_data = blob.download_blob()
                blob_data.readinto(my_blob)
            '''
        else:
            print('error', blob_filename.name)
            pass


except Exception as ex:
    print('Exception:')
    print(ex)
