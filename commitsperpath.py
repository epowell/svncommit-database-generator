#!/usr/bin/python
import sqlite3
import logging
from collections import defaultdict
from itertools import chain, ifilter

dbresult = []
committable = defaultdict(lambda: [set(), 0, 0])

def getDefectsAndPaths(connection):
    c = connection.cursor()
    c.execute('select commits.revision, paths.path, paths.lines, paths.coverage  from commits, commits_paths, paths where commitid = commits.id and paths.id = pathid;')
    res = c.fetchall() 
    c.close()
    return res

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    with sqlite3.connect('data/defectcommits.sqlite') as connection:
        dbresult = getDefectsAndPaths(connection)
    for defect, path, lines, coverage in dbresult:
        coverage = int((coverage or '0').strip(' %'))
        key = path.split('/')
        while key:
            committable[tuple(key)][1] += lines
            committable[tuple(key)][2] += coverage * lines / 100.0
            committable[tuple(key)][0].add(defect)
            key = key[:-1]
    logging.info("Done precalculating")

    #output in .csv
    print '"' + '","'.join(('# of commits', 'path', 'defect IDs', 'lines', 'covered lines', 'coverage', 'is file')) + '"'
    for key, value in sorted(committable.items(), key=lambda x: (len(x[1][0]), -len(x[0])), reverse=True):
        path = '/'.join(key)
        values = (
                    len(value[0]),
                    '"' + path + '"',
                    '"' + ','.join(value[0]) + '"',
                    value[1],
                    value[2],
                    round((value[2] * 100.0) / value[1] if value[1] else 0, 2),
                    '.' in key[-1] and not 'ZenPacks' in key[-1],
                 )
        print ','.join(map(str, values))
    logging.info("Done")


