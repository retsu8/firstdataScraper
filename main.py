from roboscraptran import Main as rt
from roboscraptran_server import Main as rts
from roboscrapcharge import Main as rc
from roboscrapstatements import Main as rs


if __name__ == '__main__':
    arv = sys.argv
    print(arv)
    if '-r' in arv:
        rt.gettransactions()
        rts.gettransactions()
        rc.getchargeback()
        rs.getstatement()

    if '-p' in arv:
        rt.parse_csv()
        rts.parse_csv()
        rc.parse_csv()
        rs.parse_csv()
        
    else:
        print("-r 'get trans' -p 'parse trans'")
