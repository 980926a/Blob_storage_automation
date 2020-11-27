import os
import uuid
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import csv
from openpyxl import Workbook
import datetime
from pytz import timezone, utc
import json
import pandas as pd
import numpy as np

columns_list = ['watch_number', 'record_day', 'record_time',
                'beacon_mac_addr_number_4', 'upload_day_KST', 'upload_time_KST', 'upload_day_China', 'upload_time_China']
azure_portal_upload_list = pd.DataFrame(columns=columns_list)

'''
필요한 기능
1) 어떤 폴더 생성 되었는지 경로랑 같이 알려주기
2) 지정한 폴더만 다운 받도록 argparse
3_1) .3gp 뒤에 문자열 붙어 있으면 문자열 제거 하는 rename 작업 & rename 지정
3_2) renamed 된 파일 이동시키고(local Download 공간에서)
3_3) local Download 된 상태에서 동일한 형상으로 Blob 컨테이너에 업로드 하기

ww
'''
new_watch_list = []
watch_sn_list = []


def cut_str(s, l):
    return [int(s[i:i+l]) for i in range(0, len(s), l)]


def strdate_to_datetime(str_date, str_time):
    _date = cut_str(str_date, 2)
    _date[0] += 2000
    _time = cut_str(str_time, 2)
    return datetime(*_date, *_time)


try:
    print("Azure Blob storage v12 - Python quickstart sample")
    # Quick start code goes here
    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
    # created after the application is launched in a console or with Visual Studio,
    # the shell or application needs to be closed and reloaded to take the
    # environment variable into account.
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    print(connect_str)

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Instantiate a ContainerClient
    container_client = blob_service_client.get_container_client(
        "smartwatchdata")

    blobs_filename_list = container_client.list_blobs()
    for blob_filename in blobs_filename_list:
        # 새로운 행 초기화
        save_file_list = list()

        '''
        파일 이름 수정해야 하는 부분
        # inhaler의 경우
        # example)
        # 20IHPA00001A/200630_\d\d\d\d\d\d_F73C21FD7B10.mp4
        # 20IHPA00001A/200701_\d\d\d\d\d\d_F73C21FD7B10.mp4
        # 20IHPA00001A/200702_\d\d\d\d\d\d_F73C21FD7B10.mp4

        # medication
        # example)
        # 20IHPA00001A/200630_\d\d\d\d\d\d_D6A00695233D.mp4
        # 20IHPA00001A/200701_\d\d\d\d\d\d_D6A00695233D.mp4
        # 20IHPA00001A/200702_\d\d\d\d\d\d_D6A00695233D.mp4


        # re.match('20IHPA00001A/200701_\d\d\d\d\d\d_F73C21FD7B10', blob_filename.name):


        '''
        if re.match('2020-11-26/20IHPA00', blob_filename.name):
            # watch_sn 중복 제거
            print(blob_filename.name, "success")
            watch_sn = blob_filename.name.split('/')[-1].split('_')[0]
            record_day = blob_filename.name.split('/')[-1].split('_')[1]
            record_time = blob_filename.name.split('/')[-1].split('_')[2]
            watch_datetime_datetime = strdate_to_datetime(
                record_day, record_time)
            str_watch_datetime_datetime = str(watch_datetime_datetime)

            beacon_mac_addr_number_4 = blob_filename.name.split(
                '/')[-1].split('_')[-1].split('.')[0][8:12]
            # KST 시간대 설정
            uploaded_Time_UTC = blob_filename.last_modified
            KST = timezone('Asia/Seoul')
            uploaded_Time_KST = uploaded_Time_UTC.astimezone(KST)

            str_uploaded_Time_KST = str(uploaded_Time_KST)
            upload_day_KST = str_uploaded_Time_KST.split(' ')[0]
            upload_time_KST = str_uploaded_Time_KST.split(
                ' ')[-1].split('+')[0]
            # China 시간대 설정
            China = timezone('Asia/Shanghai')
            uploaded_Time_China = uploaded_Time_UTC.astimezone(China)

            str_uploaded_Time_China = str(uploaded_Time_China)
            upload_day_China = str_uploaded_Time_China.split(' ')[0]
            upload_time_China = str_uploaded_Time_China.split(
                ' ')[-1].split('+')[0]

            print(uploaded_Time_KST, upload_time_China)

            if not beacon_mac_addr_number_4:
                beacon_mac_addr_number_4 = 'null'
            else:
                pass

            azure_portal_upload_list = azure_portal_upload_list.append(
                {'watch_number': watch_sn, 'record_day': record_day,
                 'record_time': record_time, 'beacon_mac_addr_number_4': beacon_mac_addr_number_4,
                 'upload_day_KST': upload_day_KST, 'upload_time_KST': upload_time_KST,
                 'upload_day_China': upload_day_China, 'upload_time_China': upload_time_China,
                 'utc': str(uploaded_Time_UTC), 'kst': str(uploaded_Time_KST), 'china': str(uploaded_Time_China)},
                ignore_index=True)
            print(azure_portal_upload_list)
            azure_portal_upload_list.to_excel(
                "finalize.xlsx", encoding='utf-8')

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
