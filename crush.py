import time
import random
import sys
from voldemort_client import Voldemort

num = 100000

def main(prefix, secs):
    secs = int(secs)
    vt = Voldemort('tcp://localhost:6666')
    c = vt.get_store('test')
    stats = {'get': [], 'put': [], 'failed': 0}
    nt = 0
    print "Running for %r seconds" % secs
    start = time.time()
    laps = set()
    while time.time() - start < secs:        
        i = random.randint(0, num)
        key = "%skey%s" % (prefix, i)
        val = ("value%s" % i) * 5000
        gs = time.time()
        v, vrs = c.get(key)
        now = time.time()
        gtaken = now - gs
        stats['get'].append((now, gtaken))
        ss = time.time()
        if not c.put(key, val): #, time=lifetime + random.randint(0, lifetime)):
            stats['failed'] += 1
        now = time.time()
        staken = now - ss
        stats['put'].append((now, staken))
        lap = (secs - (int(time.time()) - int(start)))
        if not lap % 10 and not lap in laps:
            print secs - (int(time.time()) - int(start)), "to go"
            laps.add(lap)
    evaluate_stats(stats)


def evaluate_stats(stats):
    g = [i[1] for i in stats['get']]
    g.sort()
    p = [i[1] for i in stats['put']]
    p.sort()
    gets = len(g)
    puts = len(p)

    get99 = g[int(gets * .999)-1] * 1000
    put99 = p[int(puts * .999)-1] * 1000

    print "gets: %d puts: %d failed: %d" \
          % (gets, puts, stats['failed'])
    print "get avg: %f0.3ms median: %f0.3ms 99.9: %f0.3ms" % (
        (sum(g) / float(gets)) * 1000,
        (g[gets/2]) * 1000,
        get99)
    print "put avg: %f0.3ms median: %f0.3ms 99.9: %f0.3ms" % (
        (sum(p) / float(puts)) * 1000,
        (p[puts/2]) * 1000,
        put99)

    print "gets:"
    for pct in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100):
        gp = g[int(gets * float(pct)/100.0)-1] * 1000
        print " %3d%% < %7.3fms" % (pct, gp)

    print "puts:"
    for pct in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100):
        pp = p[int(puts * float(pct)/100.0)-1] * 1000
        print " %3d%% < %7.3fms" % (pct, pp)

    gfh = open('gets.csv', 'w')
    gfh.write("time,get\n")
    for t, g in stats['get']:
        gfh.write("%s,%s\n" % (t, g))
    gfh.close()

    gfh = open('puts.csv', 'w')
    gfh.write("time,put\n")
    for t, g in stats['put']:
        gfh.write("%s,%s\n" % (t, g))
    gfh.close()




if __name__ == '__main__':
    main(*sys.argv[1:])
