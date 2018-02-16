from roboscraptran import Main as rt
from roboscraptran_server import Main as rts
from roboscrapcharge import Main as rc
from roboscrapstatements import Main as rs

import sys

if __name__ == '__main__':
    arv = sys.argv
    print(arv)
    if 'all' in arv:
        #rt_self = rt()
        #rt_self.gettransactions()
        #rt_self.parse_csv()

        rts_self = rts()
        rts_self.gettransactions()
        rts_self.parse_csv()

        rc_self = rc()
        rc_self.getchargeback()
        rc_self.parse_csv()

        #rs_self = rs()
        #rs_self.getstatement()
        #rs_self.parse_csv()

    else:
        print("all 'autorun all'")
