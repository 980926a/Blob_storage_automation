import argparse

parser = argparse.ArgumentParser(description='Argparse Tutorial')
# argument는 원하는 만큼 추가한다.
parser.add_argument('--print-number', type=int,
                    help='an integer for printing repeateably')

#args: Namespace(print_number=5)
args = parser.parse_args()


for i in range(args.pr):
    print('print number {}'.format(i+1))
