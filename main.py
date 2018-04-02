#!/bin/local/env python3
from roboscraptran_server import Main
rts = Main()
from roboscrapcharge import Main
rc = Main()
from roboscrapstatements import Main
rs = Main()
from datetime import datetime
import sys

current_date = datetime.now()

if __name__ == '__main__':
    arv = sys.argv
    print(arv)
    if 'all' in arv:
        rts.gettransactions()
        rts.parse_csv()

        rc.getchargeback()
        rc.parse_csv()

        if current_date.day >= 0 and current_date.day <= 5:
            rs.getstatement()
            rs.parse_csv()

    else:
        print("all 'autorun all'")
