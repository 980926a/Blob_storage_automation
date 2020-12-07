from logging import exception
import argparse
import os
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
from pytz import timezone
# modin 쓰고 싶은데 왜 modin 안되는지 모르겟음 ㅜㅜ
import pandas as pd
import sweetviz as sv


list_of_dataframe = list()


def blob_storage_connect(container_name):
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            connect_str)
        # Instantiate a ContainerClient
        container_client = blob_service_client.get_container_client(
            container_name)
        blobs_list = container_client.list_blobs()
        return blobs_list
    except Exception as ex:
        print('Exception:')
        print(ex)


def cut_str(s, l):
    return [int(s[i:i+l]) for i in range(0, len(s), l)]


def strdate_to_datetime(str_date, str_time):
    _date = cut_str(str_date, 2)
    _date[0] += 2000
    _time = cut_str(str_time, 2)
    return datetime(*_date, *_time)


def making_dataframe(col_dict):
    data_frame = pd.DataFrame([col_dict])
    return data_frame


def converting_df_to_excel(df, filename):
    return df.to_excel("{}.xlsx".format(filename), encoding='utf-8')


'''
def converting_df_to_html(df):
    my_report = sv.analyze(df)
    return my_report.show_html()
'''


# lambda로 변형하는것도 해야함 . filter 에 parameter 2개 던져서 하는걸 사용함
# https://wayhome25.github.io/cs/2017/04/03/cs-03/
# https://stackoverflow.com/questions/34609935/passing-a-function-with-two-arguments-to-filter-in-python/34610018

def filter_work(search_word, check_list):
    # list comprehension , map, reduce, filter, lambda 자유롭게 사용할 수 있도록 익숙해질 것
    # 정규표현식 고도화 필요
    # '2010-01-01/20IHPA' ---> blob 예시
    filtered_list = [x for x in check_list if re.match(search_word, x.name)]
    for idx, blob_attr in enumerate(filtered_list):
        if idx+1 == len(filtered_list):  # 마지막 원소
            watch_sn = blob_attr.name.split('/')[-1].split('_')[0]
            record_day_null_timezone = blob_attr.name.split(
                '/')[-1].split('_')[1]
            record_time_null_timezone = blob_attr.name.split(
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
            beacon_mac_addr_number_4 = blob_attr.name.split(
                '/')[-1].split('_')[-1].split('.')[0][8:12]

            # KST 시간대 설정
            uploaded_Time_UTC = blob_attr.last_modified
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
            diff_time_calculation = uploaded_datetime_China - record_watch_datetime_null
            str_diff_time_calcutaion = str(diff_time_calculation)

            # https://www.kite.com/python/answers/how-to-convert-a-timedelta-to-days,-hours,-and-minutes-in-python

            if not beacon_mac_addr_number_4:
                beacon_mac_addr_number_4 = 'null'
            else:
                pass

            dataframe_for_file = making_dataframe({'watch_number': watch_sn,
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
                                                   'str_diff_time_calcutaion': str_diff_time_calcutaion, })
            list_of_dataframe.append(dataframe_for_file)
            # reset_index 설정 해서 index 순서대로 들어간건지 확인 함. 설정해야지 0 , 1, 2, 3 으로 들어가고 설정 안하면 0, 0 ,0 으로 들어감
            final_dataframe = pd.concat(
                list_of_dataframe).reset_index(drop=True)
            return final_dataframe

        else:
            watch_sn = blob_attr.name.split('/')[-1].split('_')[0]
            record_day_null_timezone = blob_attr.name.split(
                '/')[-1].split('_')[1]
            record_time_null_timezone = blob_attr.name.split(
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
            beacon_mac_addr_number_4 = blob_attr.name.split(
                '/')[-1].split('_')[-1].split('.')[0][8:12]

            # KST 시간대 설정
            uploaded_Time_UTC = blob_attr.last_modified
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
            diff_time_calculation = uploaded_datetime_China - record_watch_datetime_null
            str_diff_time_calcutaion = str(diff_time_calculation)

            # https://www.kite.com/python/answers/how-to-convert-a-timedelta-to-days,-hours,-and-minutes-in-python

            if not beacon_mac_addr_number_4:
                beacon_mac_addr_number_4 = 'null'
            else:
                pass

            dataframe_for_file = making_dataframe({'watch_number': watch_sn,
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
                                                   'str_diff_time_calcutaion': str_diff_time_calcutaion, })
            list_of_dataframe.append(dataframe_for_file)


if __name__ == "__main__":
    # argparse 적용 예정
    parser = argparse.ArgumentParser(
        description="smartwatch_upload_file_check")
    # argument는 원하는 만큼 추가
    parser.add_argument(
        "blob_name", help="blob_name in the container", type=str)
    parser.add_argument("container_name", help="cotainer_name", type=str)
    parser.add_argument("save_filename", help="excel_file_name", type=str)

    args = parser.parse_args()
    # 개별 스토리지 계정 - 연결 문자열 입력(보안 유의)

    '''
    argparse 적용 예시
    '2010-01-01/20IHPA' = args.blob_name
    'smartwatchdata' = args.container_name
    '20201207_argparse_excel' = args.save_filename
    '''

    excel_source = filter_work(
        args.blob_name, blob_storage_connect(args.container_name))
    # sweetviz 패키지 사용할 경우만 활성화
    # converting_df_to_html(excel_source)
    converting_df_to_excel(excel_source, args.save_filename)
    # twine을 이용한 패키지 배포
