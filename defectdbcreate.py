#!/usr/bin/python
from svnparser import parseFile, Commit
import sqlite3
import logging
from itertools import chain, ifilter

excludedRevisions = ['r50360']

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
     UNIQUE (revision) ON CONFLICT REPLACE
    )
    ''')
    c.execute('''create table if not exists paths
    (id integer primary key,
     path text,
     UNIQUE (path) ON CONFLICT REPLACE
    )''')
    c.execute('''create table if not exists defects
    (id integer primary key,
     defect text,
     UNIQUE (defect) ON CONFLICT REPLACE
    )''')
    c.execute('''create table if not exists commits_paths
    (id integer primary key,
     commitid integer,
     pathid integer,
     FOREIGN KEY(commitid) REFERENCES commits(id),
     FOREIGN KEY(pathid) REFERENCES paths(id),
     UNIQUE (commitid, pathid) ON CONFLICT REPLACE
    )''')
    c.execute('''create table if not exists commits_defects
    (id integer primary key,
     commitid integer,
     defectid integer,
     FOREIGN KEY(commitid) REFERENCES commits(id),
     FOREIGN KEY(defectid) REFERENCES defects(id),
     UNIQUE (commitid, defectid) ON CONFLICT REPLACE
    )''')
    # Save (commit) the changes
    connection.commit()
    c.close()

def insertAndGetPathId(connection, path=None):
    return insertAndGetId(connection, 'paths', 'path', path)

def insertAndGetDefectId(connection, defect=None):
    if defect: defect = defect.upper()
    return insertAndGetId(connection, 'defects', 'defect', defect)

def insertAndGetId(connection, tablename, columnname, value=None):
    if not value:
        raise ValueError(value)
    value = value.strip()

    c = connection.cursor()
    try:
        c.execute('insert into %s values (NULL, ?)' % tablename, (value,))
        if not c.lastrowid: raise Exception('%s not found after insert' % columname)
        return c.lastrowid
    finally:
        c.close()
        
def insertAndGetCommitId(connection, value=None):
    if not value:
        raise ValueError(value)

    c = connection.cursor()
    try:
        c.execute('insert into commits values (NULL, ?, ?, ?)', (value.date, value.revision, value.message))
        if not c.lastrowid: raise Exception('%s not found after insert' % columname)
        return c.lastrowid
    finally:
        c.close()

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

            commitId = insertAndGetCommitId(connection, commit)

            pathIds = map(lambda x: insertAndGetPathId(connection, x), commit.paths)
            defectIds = map(lambda x: insertAndGetDefectId(connection, x), commit.defects)

            c = connection.cursor()
            for pathId in pathIds:
                c.execute('insert into commits_paths values (NULL, ?, ?)', (commitId, pathId))
            for defectId in defectIds:
                c.execute('insert into commits_defects values (NULL, ?, ?)', (commitId, defectId))
            connection.commit()
            c.close()

        logging.info("%d commits indexed", len(goodcommits))

