from logging import exception
import argparse
import os
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
from pytz import timezone
# modin 쓰고 싶은데 왜 modin 안되는지 모르겟음 ㅜㅜ
# 패키지 호환 문제 인듯 pip 내
import pandas as pd


# 연결문자열 입력 여부 확인 : echo %AZURE_STORAGE_CONNECTION_STRING%


# 입력된 환경변수가 있는 경우 return 값 시작이 %로 시작 안함
# 입력된 환경변수가 없는 경우 retrun 값 시작이 %로 시작함
# 연결문자열을 입력하도록 해야함
# 연결문자열 제대로 입력한 경우
# SUCCESS: Specified value was saved. 메시지 출력됨

'''
프로그램 다시 시작
환경 변수가 추가되면 이 환경 변수를 읽어야 하는 실행 중인 프로그램을 다시 시작합니다. 예를 들어 개발 환경 또는 편집기를 다시 시작한 후에 계속합니다.
'''
'''
12월 10일 작업 예정
# cmd 창 내 명령어 실행하는 법 - subprocess, os
connect_str = input("Azure portal 내 해당 스토리지 계정의 연결 문자열을 입력하세요:")
os.system('setx AZURE_STORAGE_CONNECTION_STRING {}'.format(connect_str))
'''


list_of_dataframe = list()


# git prune 정확하게 알고 사용하기
# git pull 햇을때 최신 파일 안가져와서 git commit 하려니 최신파일이라고 오류났음
# git prune 하고 git pull 하니 촤신 가지고 왔음
# git pull default 값 알기
# https://blog.leocat.kr/notes/2016/01/21/git-config-default-remote-branch
# 경준 추가
#

# 스토리지 계정 - 현재 devdatas 랑 chinaproject가 있음
# 스토리지 계정이랑 AZURE_STORAGE_CONNECTION_STRING.split(';')[1].split('=')[-1] 비교해서
#


# logic A : 입력되어 있는 연결 문자열도 검증
# logic B : 연결 문자열이 없는 경우는 연결 문자열 입력을 받고, 연결 문자열


# 입력값 재입력하게 할 경우 초기화하는 과정 필요함!!!
# debug 모드에서 문제가 됨!
# 왜 debug 모드에서 문제가 되냐면, 가상환경 모드를 실행시키는 부분의 'C:/ProgramData/Anaconda3/Scripts/activate' 이 명령문이 변수로 들어갈 수 있어서!
# 파이썬 클래스 공부!


def blob_storage_connect(container_name):
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        print(connect_str)
        print('등록된 연결 문자열이 있습니다.')
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            connect_str)
        # Instantiate a ContainerClient
        container_client = blob_service_client.get_container_client(
            container_name)
        blobs_list = container_client.list_blobs()
        return blobs_list
    except:

        '''
        class 사용해서 변수 값 초기화 하는 거 __init__
        magic method 공부할 것
        '''

        print('등록된 연결 문자열이 없습니다.')
        connect_str = input("Azure portal 내 해당 스토리지 계정의 연결 문자열을 입력하세요:")
        if connect_str.startswith('DefaultEndpointsProtocol'):
            os.system(
                'setx AZURE_STORAGE_CONNECTION_STRING {}'.format(connect_str))
            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connect_str)
                container_client = blob_service_client.get_container_client(
                    container_name)
                blobs_list = container_client.list_blobs()
                return blobs_list
            except Exception as ex:
                print('Exception:')
                print(ex)
        else:
            connect_str = input("잘못된 연결문자열을 입력하셨습니다. 확인하시고 다시 입력하세요:")
            os.system(
                'setx AZURE_STORAGE_CONNECTION_STRING {}'.format(connect_str))
            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connect_str)
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

# https://blockdmask.tistory.com/429

# 예외 처리 만들어야 함
# 데이터 없는 경우
#


def converting_df_to_excel(df, filename):
    try:
        print('excel 파일을 만들고 있습니다.')
        return df.to_excel(f'{filename}.xlsx', encoding='utf-8')
        # return df.to_excel("{}.xlsx".format(filename), encoding='utf-8')
    except:
        print('데이터가 없는 것으로 파악됩니다')


#  return df.to_excel(f'{filename}.xlsx',encoding='utf-8')
'''
def converting_df_to_html(df):
    my_report = sv.analyze(df)
    return my_report.show_html()
'''


# lambda로 변형하는것도 해야함 . filter 에 parameter 2개 던져서 하는걸 사용함
# https://wayhome25.github.io/cs/2017/04/03/cs-03/
# https://stackoverflow.com/questions/34609935/passing-a-function-with-two-arguments-to-filter-in-python/34610018
# if else in a list comprehension [duplicate]
# https://stackoverflow.com/questions/4406389/if-else-in-a-list-comprehension
# [x+1 if x >= 45 else x+5 for x in l]

# 작성중 (주말 할 예정 . list comprehension 내 else 없이 작성한거 확인해야함 V)
# 원래 코드 : [x for x in check_list if re.match(search_word, x.name)]
# 1) re.match를 통과한


def filter_work(search_word, check_list, country):
    # list comprehension , map, reduce, filter, lambda 자유롭게 사용할 수 있도록 익숙해질 것
    # 정규표현식 고도화 필요
    # '2010-01-01/20IHPA' ---> blob 예시
    # [x if re.match(search_word, x.name)for x in check_list ]
    # [x for x in check_list if re.match(search_word, x.name) else pass]

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

            # watch가 사용중인 시간대를 나라 입력을 받아서 분기 해 줌
            # 우선은 중국을 default로 설정
            if country == 'korea':
                record_watch_time = timezone('Asia/Seoul')
            elif country == 'japan':
                record_watch_time = timezone('Asia/Tokyo')
            else:
                record_watch_time = timezone('Asia/Shanghai')

            # watch 시간대에 따라서 변경
            record_watch_datetime_null = watch_datetime_datetime_null.astimezone(
                record_watch_time)
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
            # https://stackoverflow.com/questions/20167930/start-index-at-1-for-pandas-dataframe/45883232
            final_dataframe.index += 1
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

            # watch가 사용중인 시간대를 나라 입력을 받아서 분기 해 줌
            # 우선은 중국을 default로 설정
            if country == 'korea':
                record_watch_time = timezone('Asia/Seoul')
            elif country == 'japan':
                record_watch_time = timezone('Asia/Tokyo')
            else:
                record_watch_time = timezone('Asia/Shanghai')

            # watch 시간대에 따라서 변경
            record_watch_datetime_null = watch_datetime_datetime_null.astimezone(
                record_watch_time)
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
    # argparse 적용 완료
    # argparse 공부할때 참고한 블로그
    # https://newsight.tistory.com/76 정리 잘되어 잇음
    '''
    augment-data-length 이렇게 된 argument는 parsing되고나면 저절로 augment_data_length 즉 '-'이 '_'으로 바뀌면서 변수명이 정해진다.

    https://github.com/python/cpython/blob/0cd5bff6b7da3118d0c5a88fc2b80f80eb7c3059/Lib/argparse.py#L1556    
    '''

    parser = argparse.ArgumentParser(
        description="smartwatch_upload_file_check")
    # argument는 원하는 만큼 추가
    parser.add_argument(
        "blob_name", help="blob_name in the container", type=str)
    parser.add_argument("container_name", help="cotainer_name", type=str)
    parser.add_argument(
        "timezone", help="country where the watch is used", type=str)
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
        args.blob_name, blob_storage_connect(args.container_name), args.timezone)
    # sweetviz 패키지 사용할 경우만 활성화
    # converting_df_to_html(excel_source)
    converting_df_to_excel(excel_source, args.save_filename)
    # twine을 이용한 패키지 배포
