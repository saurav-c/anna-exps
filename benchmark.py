from anna.client import AnnaTcpClient as ATC
from anna.client import AnnaTcpClient as ATC
from anna.lattices import LWWPairLattice as LWW
import numpy as np
import scipy.stats as stats
import time
import sys
import os

NUM = 100
N = 1000
x = np.arange(1, N)

def main():
    a = float(sys.argv[1])
    w = int(sys.argv[2])
    r = int(sys.argv[3])
    elb = sys.argv[4]
    ip = sys.argv[5]
    weights = x ** (-a)
    weights /= weights.sum()
    bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))
    lats = run(bounded_zipf, w, r, elb, ip)
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

def run(gen, writes, reads, elb, ip):
    c = ATC(elb, ip)
    val = os.urandom(4096)
    lat = []
    for i in range(NUM):
        t = 0
        for j in range(writes):
            key = str(gen.rvs(size=1)[0])
            lww = LWW(time.time_ns(), val)
            s = time.time()
            c.put(key, lww)
            e = time.time()
            t += (e-s) * 1000
        for j in range(reads):
            key = str(gen.rvs(size=1)[0])
            s = time.time()
            r = c.get(key)
            e = time.time()
            t += (e-s) * 1000
            if len(r) < 1:
                print('Error reading key %s' % (key))
                print(r)
        lat.append(t)
    return lat
if __name__ == '__main__':
    main()