import argparse

parser = argparse.ArgumentParser(description='Argparse Tutorial')
# argument는 원하는 만큼 추가한다.
parser.add_argument('--container-name', type=int,
                    help='an integer for printing repeateably')

# parser 라는 객체를 만들고 거기에 인자를 추가하는 형태인데, args.(추가된 인자가 생성되는 방식-- 파생된 개념 Namespace)
# 그래서 Nmaespace가 생성되는 규칙이 있는 것 같았음

'''
debug_mode에서 "args": ["--container-name","5"]
인자 전달할때 리스트 형태로 전달해야함

'''


'''
## Argparse 자체 파싱 규칙
https://newsight.tistory.com/76
augment-data-length 이렇게 된 argument는 parsing되고나면 저절로 augment_data_length 즉 '-'이 '_'으로 바뀌면서 변수명이 정해진다.
https://github.com/python/cpython/blob/0cd5bff6b7da3118d0c5a88fc2b80f80eb7c3059/Lib/argparse.py#L1556

'''


# vars(args)를 하면 알수 있음.
# args: Namespace(print_number=5) 형태로 전달됨
args = parser.parse_args()


for i in range(args.container_name):
    print('print number {}'.format(i+1))
