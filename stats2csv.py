#!/usr/bin/env python

import csv
import os
import sys
import cPickle as pickle

def main():
    names, stats = load_stats_files()
    get_cols = [ 'get %s' % n for n in names ]
    put_cols = [ 'put %s' % n for n in names ]
    cols = get_cols + put_cols
    writer = csv.DictWriter(open('stats.csv', 'w'), cols)
    writer.writerows(rows(names, stats))


def load_stats_files():
    names = []
    stats = []
    fnames = sys.argv[1:]
    for fn in fnames:
        stats.append(pickle.load(open(fn, 'r')))        
        names.append(fn)
    return names, stats


def rows(names, stats):
    r = 0
    while True:
        row = {}
        for ix, s in enumerate(stats):
            get_col = 'get %s' % names[ix]
            put_col = 'put %s' % names[ix]
            try:
                row[get_col] = s['get'][r]
            except IndexError:
                row[get_col] = None
            try:
                row[put_col] = s['put'][r]
            except IndexError:
                row[put_col] = None
        any = False
        for k, v in row.items():
            if v is not None:
                any = True
                break
        if not any:
            raise StopIteration
        yield row
        if r % 100 == 0:
            print r,
        r = r + 1


if __name__ == '__main__':
    main()
        
