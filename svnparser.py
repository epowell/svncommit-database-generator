#!/usr/bin/python
import re
import sys
import logging
from collections import namedtuple
from itertools import chain

#'------------------------------------------------------------------------'
DASHLINE = '-'*72
COMMIT_PATH_REGEX = re.compile(r'^(?P<operation>[MADR]) (?P<path>/[^\(]*)(?: \(from [^\)]*\))?')
TRAC_DEFECT_IDS_REGEX = re.compile(r'[^-_rvSALETFROC/\[\(\)]{1,2}(?P<id>[1-9][0-9]{3,4})', flags=re.I)
JIRA_DEFECT_IDS_REGEX = re.compile(r'(?:ZE[NM]|JIRA|FIXES)[-_]?(?P<id>[0-9]{1,4})', flags=re.I)

Commit = namedtuple('Commit', ['revision', 'date', 'message', 'paths', 'defects'])

def extractDefectsFromMessage(message):
    return [x.group('id') for x in chain(TRAC_DEFECT_IDS_REGEX.finditer(message), JIRA_DEFECT_IDS_REGEX.finditer(message))]
    #return DEFECT_IDS_REGEX.findall(message)

def parseCommit(rawcommit):
    """
    A commit looks like this:

    r66969 | smousa | 2012-12-05 14:02:29 -0600 (Wed, 05 Dec 2012) | 1 line
    Changed paths:
       M /trunk/core
       M /trunk/core/Products
       M /trunk/core/Products/ZenModel/ZenossInfo.py

    fixes zen-4387. Enables remote hub logs to be viewed on the UI.  Reviewed by Jason Peacock
    """
    if not rawcommit:
        return None

    #split lines
    commitlines = rawcommit.splitlines()
    if commitlines[0] == DASHLINE:
        commitlines = commitlines[1:]
    if commitlines[-1] == DASHLINE:
        commitlines = commitlines[:-1]
    if commitlines[1].strip() != 'Changed paths:':
        raise Exception('Commit format problem: no path list found')

    commitdata = map(lambda x: x.strip(), commitlines[0].split('|'))

    numMessageLines = int(commitdata[-1].split()[0])
    message = '\n'.join(commitlines[-numMessageLines:])

    defects = extractDefectsFromMessage(message)

    pathlines = filter(None, map(lambda x: x.strip(), commitlines[2:-numMessageLines]))
    paths = map(lambda x: COMMIT_PATH_REGEX.match(x).group('path'), pathlines)

    return Commit(commitdata[0], commitdata[2], message, paths, defects)

def isInterestingCommit(commit):
    if not commit:
        logging.debug("No commit passed")
        return False
    if int(commit.revision[1:]) < 48193:
        logging.debug("Discarding commit %s - too early", commit.revision)
        return False
    if len(commit.defects) == 0:
        logging.debug("Discarding commit %s - no defects in '%s'", commit.revision, commit.message)
        return False
    return True

def parseFile(filename):
    with open(filename) as infile:
        return parse(infile)

def parse(stream):
    rawlog = stream.read()
    rawcommits = rawlog.split('\n' + DASHLINE + '\n')
    if not rawcommits[0]:
        rawcommits = rawcommits[1:]
    if not rawcommits[-1]:
        rawcommits = rawcommits[:-1]
    logging.info("Found %d commits", len(rawcommits))
    commits = filter(isInterestingCommit, map(parseCommit, rawcommits))
    logging.info("Found %d interesting commits", len(commits))
    return commits

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    if len(sys.argv)<2:
        logging.error("Input SVN log required")
        exit(1)
    parseFile(sys.argv[1])

