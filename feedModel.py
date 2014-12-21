"""
Feed processor module:

Classes Supplied:
    Feed - abstract base class, extend before using
    CsvFeed - abstract subclass of Feed for csv files

Synopsis:
Feed(object):
    Abstract base class
    Templating (AKA Self Delegation) Design Patten. All hail Alex Martelli
    Constructor takes source file name (string) and db connection (python-mysqldb cursor)

    ___ Attributes ___:
        tableName (MUST BE OVERRIDDEN) - override this with the name of the db table
        validators (OPTIONAL) - override this with a list of callables,
            corresponding with the table's columns.
            --Each validator should take a value and return a value if the
                incoming argument is valid, otherwise return None.
            --The value returned should be SQL ready (i.e: strings in double quotes).
                Otherwise, override self.listToMySQL() and include logic to prepare SQL.

    ___ Organising Methods ___:
        fetchFromFile(self, cursor, fileObject) - process a stringIO fileObject
        handleBadLine(self, badLine, reason) (OPTIONAL) - does stuff to bad lines
        handleSQLRejection(self) (OPTIONAL) - called if the server returns an SQL
            error for the bulk MySQL INSERT. Splits the bulk INSERT into smaller
            statements and executes each one separately.

    ___ Hook Methods ___:
        lineToList(self, line) (MUST BE OVERRIDDEN) - process line from source
            file (in string format) into a list of values.
        listToMySQL(self, l) (OPTIONAL) - take of values, return the VALUES sections
            of a MySQL INSERT statement

csvFeed(Feed):
    Semi-concrete subclass for CSV files. (only semi because tableName has to be overidden)
    HIGHLY RECOMMENDED to override 'validators' with a list of callable validators
        corresponding with the table's columns.
"""

import json
import csv
import StringIO

from settings import splitFactor


class Feed(object):
    """
    Abstract base class
    Templating (AKA Self Delegation) Design Patten. All hail Alex Martelli
    """
    tableName = 'ABSTRACT__-__NotImplementedError'
    cursor = None
    fileObject = None
    firstLine = None
    validators = None
    segments = None
    badLines = None

    def __init__(self):
        """ Constructor """
        pass

    """___ Organising Methods ___"""
    def fetchFromFile(self, cursor=None, fileObject=None):
        """Process fileObject into MySQL insert statement, pass to MySQL"""
        self.cursor = cursor
        self.fileObject = fileObject
        self.segments = list()
        self.badLines = list()
        for line in self.fileObject.readlines():
            l = self.lineToList(line)
            if not l:
                continue  # self.lineToList populated self.firstLine, continue
            # Validation:
            # If the line has too few or too many values:
            if not len(l) == len(self.firstLine):
                self.handleBadLine(line, 'Wrong number of values')
                continue
            if len(self.validators) == len(self.firstLine):
                validatedL = list()
                for validator, item in zip(self.validators, l):
                    validatedL.append(validator(item))
                    if validatedL[-1] is None:  # If the item was invalid:
                        self.handleBadLine(line, 'Value rejected:  ' + item)
                        continue
                l = validatedL
            self.segments.append(self.listToMySQL(l))
        self.executeQuery()

    def executeQuery(self, segments=None,):
        """Use the SQL segments in self.segments to create a bulk INSERT statement
        Calls  self.handleSQLRejection() if the server rejects the statement"""
        if not segments: segments = self.segments
        query =''.join(['INSERT INTO ', self.tableName, ' (',
            ', '.join(self.firstLine), ') VALUES ',
            ', '.join([segment for segment in segments]), ';'])
        try:
            self.cursorExecute(query)
        except:
            print 'Bulk query rejected, splitting into smaller queries.'
            self.handleSQLRejection(0, len(self.segments))

    def handleSQLRejection(self, lowerL, upperL):
        """Split the bulk query into smaller queries
        Recursively attempt the smaller queries, spliting further if they fail.
        Print out the identified invalid lines.
        """
        segmentLength = int((upperL-lowerL)/(splitFactor-1))
        for i in xrange(lowerL, upperL, segmentLength):
            upper = i + segmentLength if i + segmentLength < upperL else upperL
            lower = i
            query = ''.join(['INSERT INTO ', self.tableName, ' (',
                ', '.join(self.firstLine), ') VALUES ',
                ', '.join([segment for segment in self.segments[lower:upper]]),
                ';'])
            try:
                self.cursorExecute(query)
            except:
                if upper - lower <= 1: # If a single line failed:
                    print ''.join(['FAILED ', str(upper), ': ',
                        json.dumps(self.segments[lower])])
                else:
                    self.handleSQLRejection(lower, upper)
                    

    """___ Hook Methods ___"""
    def lineToList(self, line):
        """Action to take for each line in feed file"""
        raise NotImplementedError

    def handleBadLine(self, badLine, reason=None):
        """Do Stuff, log the lines into some log file or something."""
        if not reason: reason='N/A'
        d = dict(reason=reason, bad_Line=badLine)
        self.badLines.append(d)

    def listToMySQL(self, l):
        """Takes list, makes INSERT DB query"""
        return ''.join(['(', ', '.join(l), ')'])

    """___ Wrapper Methods ___"""
    def cursorExecute(self, query):
        """Wrapper method for self.cursor"""
        return self.cursor.execute(query)


class VirginTrainsCsvDialect(csv.Dialect):
    """Dialect matching the feed CSVs"""
    strict = True
    skipinitialspace = True
    delimiter = ','
    quotechar = '"'
    quoting=csv.QUOTE_NONE
    lineterminator = '\n'


class CsvFeed(Feed):
    """
    Abstract subclass for CSV files (only override tableName and the validators
        if you are in a rush).
    Templating (AKA Self Delegation) Design Patten. All hail Alex Martelli
    """
    CSVDialect = VirginTrainsCsvDialect()

    def lineToList(self, line):
        """Action to take for each line of <bold>CSV<bold> feed file"""
        l = [item for item in next(csv.reader(StringIO.StringIO(line), self.CSVDialect))]
        if self.firstLine is None:
            self.firstLine = l
            return None
        return l
