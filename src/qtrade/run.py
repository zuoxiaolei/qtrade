import sys
from update_etf_data import run_every_day



def main():
    method = sys.argv[1]
    other_args = ''
    if len(sys.argv) > 2:
        other_args = sys.argv[2:]
        other_args = ','.join(other_args)
    eval_string = f"{method}({other_args})"
    eval(eval_string)


main()
