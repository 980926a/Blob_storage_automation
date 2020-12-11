import os
connect_str = input("Azure portal 내 해당 스토리지 계정의 연결 문자열을 입력하세요:")
os.system('cd {}'.format(connect_str))
os.system('setx AZURE_STORAGE_CONNECTION_STRING {}'.format(connect_str))
