import sys
sys.path.append('./../anna/client/python')


from anna.client import AnnaTcpClient as ATC
from anna.client import AnnaTcpClient as ATC
from anna.lattices import LWWPairLattice as LWW

import argparse
import numpy as np
import scipy.stats as stats
import time
import os

def main():

    parser = argparse.ArgumentParser(description='Anna Benchmark Trigger')
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for Anna', 
                        dest='address', required=True)
    parser.add_argument('-i', '--ip', nargs=1, type=str, metavar='A',
                        help='My IP Address', 
                        dest='ip', required=True)
    parser.add_argument('-t', '--txn', nargs=1, type=int, metavar='Y',
                        help='The number of txns to be done.',
                        dest='txn', required=True)
    parser.add_argument('-r', '--reads', nargs=1, type=int, metavar='Y',
                        help='The number of reads to be done.',
                        dest='reads', required=True)
    parser.add_argument('-w', '--writes', nargs=1, type=int, metavar='Y',
                        help='The number of writes to be done.',
                        dest='writes', required=True)
    parser.add_argument('-z', '--zipf', nargs='?', type=float, metavar='Y',
                        help='Zipfian coefficient',
                        dest='zipf', required=False, default=1.0)
    parser.add_argument('-p', '--pre', nargs='?', type=str, metavar='Y',
                        help='Prefix key',
                        dest='prefix', required=False, default='tasc')
    parser.add_argument('-n', '--numkeys', nargs='?', type=int, metavar='Y',
                        help='Keyspace to choose from',
                        dest='knum', required=False, default=1000)
    parser.add_argument('-d', '--debug', nargs='?', type=bool, metavar='Y',
                        help='Whether or not to debug',
                        dest='debug', required=False, default=False)
    parser.add_argument('-bw', '--Benchmark', nargs='?', type=bool, metavar='Y',
                        help='Warmup Benchmark',
                        dest='warmup', required=False, default=False)
    args = parser.parse_args()

    warmup = args.warmup
    elb = args.address[0]
    ip = args.address[0]
    num_txn = args.txn[0]
    num_reads = args.reads[0]
    num_writes = args.writes[0]
    z = args.zipf
    prefix = args.prefix
    num_keys = args.knum
    debug = args.debug

    x = np.arange(1, num_keys)
    weights = x ** (-z)
    weights /= weights.sum()
    bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))

    num_txn = num_txn if not warmup else num_keys
    lats, errs = run(bounded_zipf, elb, ip, num_txn, num_reads, num_writes, prefix, debug, warmup)

    # Ignore 1st 10%
    lats = lats[(num_txn // 10):]
    lats = np.array(lats)

    med = np.percentile(lats, 50)
    one = np.percentile(lats, 1)
    five = np.percentile(lats, 5)
    ninefive = np.percentile(lats, 95)
    ninenine = np.percentile(lats, 99)

    print('Median: {}'.format(med))
    print('1st {}'.format(one))
    print('5th: {}'.format(five))
    print('95th: {}'.format(ninefive))
    print('99th: {}'.format(ninenine))
    print('Error Count: {}'.format(errs))

def run(gen, elb, ip, num_txns, num_writes, num_reads, prefix, debug, warmup):
    c = ATC(elb, ip)
    print('Connected to Anna with ELB {} and my IP {}'.format(elb, ip))

    val = os.urandom(4096)
    lat = []
    errs = 0

    print('Performing {} transactions of {} writes and {} reads'.format(num_txns, num_writes, num_reads))

    for i in range(num_txns):
        t = 0
        for j in range(num_writes):
            key = prefix + str(gen.rvs(size=1)[0]) if not warmup else prefix + str(num_txns)
            lww = LWW(time.time_ns(), val)
            s = time.time()
            resp = c.put(key, lww)
            e = time.time()
            t += (e - s)

            # Error Check
            if not resp or not all(resp.values()):
                errs += 1
                if debug:
                    print('Error writing %s' % (key))
                    print('Response: {}'.format(resp))

        for j in range(num_reads):
            key = prefix + str(gen.rvs(size=1)[0])
            s = time.time()
            resp = c.get(key)
            e = time.time()
            t += (e - s)
            
            if len(resp) < 1:
                errs += 1
                if debug:
                    print('Error reading %s' % (key))
                    print('Response: {}'.format(resp))
        lat.append(t)

    convert = lambda x: x * 1000
    lat = list(map(convert, lat))
    
    return (lat, errs)


if __name__ == '__main__':
    main()