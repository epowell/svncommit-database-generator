#!/usr/bin/python
import sqlite3
import logging
from collections import defaultdict
from itertools import chain, ifilter

dbresult = []
committable = defaultdict(set)

def getDefectsAndPaths(connection):
    c = connection.cursor()
    c.execute('select commits.revision, paths.path from commits, commits_paths, paths where commitid = commits.id and paths.id = pathid;')
    res = c.fetchall() 
    c.close()
    return res

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    with sqlite3.connect('data/defectcommits.sqlite') as connection:
        dbresult = getDefectsAndPaths(connection)
    for defect, path in dbresult:
        key = path.split('/')
        while key:
            committable[tuple(key)].add(defect)
            key = key[:-1]
    logging.info("Done precalculating")
    for key, value in sorted(committable.items(), key=lambda x: (len(x[1]), -len(x[0])), reverse=True):
        path = '/'.join(key)
        print str(len(value)) + ', ' + path + ', "' + ','.join(value) + '", ' + str('.' in key[-1] and not 'ZenPacks' in key[-1])
    logging.info("Done")

