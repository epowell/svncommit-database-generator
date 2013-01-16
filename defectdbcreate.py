#!/usr/bin/python
from svnparser import parseFile, Commit
from coverage_102312 import Coverage, getCoverage
import sqlite3
import logging
from itertools import chain, ifilter

excludedRevisions = ['r50360']
latestInsertedId = -1
tableprimaries = {'commits': 'revision', 'paths': 'path', 'defects': 'defect'}

def cleanDb(connection):
    c = connection.cursor()
    # Create tables
    c.execute('''drop table if exists commits''')
    c.execute('''drop table if exists paths''')
    c.execute('''drop table if exists defects''')
    c.execute('''drop table if exists commits_paths''')
    c.execute('''drop table if exists commits_defects''')
    # Save (commit) the changes
    connection.commit()
    c.close()

def createTables(connection):
    c = connection.cursor()
    # Create tables
    c.execute('''create table if not exists commits
    (id integer primary key,
     date text,
     revision text,
     message text,
     UNIQUE (revision) ON CONFLICT IGNORE
    )
    ''')
    c.execute('''create table if not exists paths
    (id integer primary key,
     path text,
     lines integer default 0,
     coverage integer default 0,
     UNIQUE (path) ON CONFLICT IGNORE
    )''')
    c.execute('''create table if not exists defects
    (id integer primary key,
     defect text,
     UNIQUE (defect) ON CONFLICT IGNORE
    )''')
    c.execute('''create table if not exists commits_paths
    (id integer primary key,
     commitid integer,
     pathid integer,
     FOREIGN KEY(commitid) REFERENCES commits(id),
     FOREIGN KEY(pathid) REFERENCES paths(id),
     UNIQUE (commitid, pathid) ON CONFLICT IGNORE
    )''')
    c.execute('''create table if not exists commits_defects
    (id integer primary key,
     commitid integer,
     defectid integer,
     FOREIGN KEY(commitid) REFERENCES commits(id),
     FOREIGN KEY(defectid) REFERENCES defects(id),
     UNIQUE (commitid, defectid) ON CONFLICT IGNORE
    )''')
    # Save (commit) the changes
    connection.commit()
    c.close()

def insertAndGetPathId(connection, path=None):
    pathcoverage = getCoverage(path)
    if pathcoverage.lines == 0 and pathcoverage.coverage == 0 and '.' not in path:
        #don't insert directories
        return None

    columndict = {
        'path': path,
        'lines': pathcoverage.lines,
        'coverage': pathcoverage.coverage
    }
    return insertAndGetId(connection, 'paths', columndict)

def insertAndGetDefectId(connection, defect=None):
    if defect: defect = defect.upper()
    return insertAndGetId(connection, 'defects', {'defect': defect})

def getIdFromPrimaryColumn(cursor, tablename, columndict):
    cursor.execute('select id from %s where %s = ?' % (tablename, tableprimaries[tablename]), (columndict[tableprimaries[tablename]],))
    return cursor.fetchone()

def insertAndGetId(connection, tablename, columndict={}):
    c = connection.cursor()
    try:
        c.execute('insert into %s (id, %s) values (NULL, %s)' % (tablename, ', '.join(columndict.keys()), ', '.join('?'*len(columndict))), columndict.values())
        insertedId = getIdFromPrimaryColumn(c, tablename, columndict)
        if not insertedId: raise Exception('%s not found after insert' % columdict)
        return insertedId[0]
    finally:
        c.close()
        
def insertAndGetCommitId(connection, commit=None):
    if not commit:
        raise ValueError(commit)

    columndict = {
        'date': commit.date or '',
        'revision': commit.revision,
        'message': commit.message or ''
    }

    return insertAndGetId(connection, 'commits', columndict)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    with sqlite3.connect('data/defectcommits.sqlite') as connection:
        cleanDb(connection)

        logging.info("Creating tables...")
        createTables(connection)
        
        logging.info("Parsing files...")
        allcommits = chain(parseFile('data/trunk_core_commits.log'), parseFile('data/trunk_ent_commits.log'))
        goodcommits = filter(lambda c: c.revision not in excludedRevisions, allcommits)
        
        logging.info("Processing commits...")
        for commit in goodcommits:
            #if commit.revision == 'r65357':
            #    import pdb; pdb.set_trace()

            commitId = insertAndGetCommitId(connection, commit)

            pathIds = filter(None, map(lambda x: insertAndGetPathId(connection, x), commit.paths))
            defectIds = filter(None, map(lambda x: insertAndGetDefectId(connection, x), commit.defects))

            c = connection.cursor()
            for pathId in pathIds:
                c.execute('insert into commits_paths values (NULL, ?, ?)', (commitId, pathId))
            for defectId in defectIds:
                c.execute('insert into commits_defects values (NULL, ?, ?)', (commitId, defectId))
            connection.commit()
            c.close()

        logging.info("%d commits indexed", len(goodcommits))

