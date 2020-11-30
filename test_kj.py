import os
import uuid
import re
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
from pytz import timezone, utc
import pandas as pd

sum_dataframe = list()


def blob_storage_connect(container_name):
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # Instantiate a ContainerClient
    container_client = blob_service_client.get_container_client(container_name)
    blobs_filename_list = container_client.list_blobs()
    return blobs_filename_list


def cut_str(s, l):
    return [int(s[i:i+l]) for i in range(0, len(s), l)]


def strdate_to_datetime(str_date, str_time):
    _date = cut_str(str_date, 2)
    _date[0] += 2000
    _time = cut_str(str_time, 2)
    return datetime(*_date, *_time)


def making_dataframe(data_frame):
    columns_list = ['watch_number']
    data_frame = pd.DataFrame(columns=columns_list)
    return data_frame


def adding_data_to_datafrmae(col_dict, dataframe_name):
    dataframe = making_dataframe(dataframe_name)
    dataframe = dataframe.append(col_dict, ignore_index=True)
    return dataframe


def blob_info(container_name, selected_watch):
    blobs_filename_list = blob_storage_connect(container_name)
    for blob_filename in blobs_filename_list:
        if re.match(selected_watch, blob_filename.name):
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
            dataframe_for_file = adding_data_to_datafrmae({'watch_number': watch_sn,
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
                                                           'str_diff_time_calcutaion': str_diff_time_calcutaion, }, 'made_by_kyungjun')

            '''
            수정해야하는 영역
            현재 문제점: for 반복문을 돌면서 행이 계속 쌓여야 하는데
            행이 안 쌓임
            s = dataframe_for_file.to_excel
            return s
            '''

        else:
            print('error', blob_filename.name)
            pass


if __name__ == "__main__":
    blob_storage_connect("smartwatchdata")
    blob_info("smartwatchdata", "2020-11-24/20IHPA")
