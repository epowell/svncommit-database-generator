#!/usr/bin/python
from svnparser import parseFile, Commit
import sqlite3

excludedRevisions = ['r50360']

def createTables(connection):
    c = connection.cursor()
    # Create tables
    c.execute('''create table if not exists commits
    (id integer primary key,
     date text,
     revision text,
     message text)
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
    c.execute('insert into %s values (NULL, ?)' % tablename, (value,))
    c.execute('select id from %s where %s=?' % (tablename, columnname), (value,))
    res = c.fetchone()
    if not res:
        c.close()
        raise Exception('%s not found after insert!' % columname)
    connection.commit()
    c.close()
    return res[0]
    
def insertAndGetCommitId(connection, value=None):
    if not value:
        raise ValueError(value)

    c = connection.cursor()
    c.execute('insert into commits values (NULL, ?, ?, ?)', (value.date, value.revision, value.message))
    c.execute('select id from commits where revision=?', (value.revision,))
    res = c.fetchone()
    if not res:
        c.close()
        raise Exception('Commit not found after insert!')
    connection.commit()
    c.close()
    return res[0]

with sqlite3.connect('defectcommits.sqlite') as connection:
    createTables(connection)

    allcommits = parseFile('trunk_core_commits.log') + parseFile('trunk_ent_commits.log')
    goodcommits = filter(lambda c: c.revision not in excludedRevisions, allcommits)
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

    print len(goodcommits)


