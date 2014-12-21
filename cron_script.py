import json
import MySQLdb

import settings
from feedModel import initialiseRemoteDB, Feed, CsvFeed
from getFiles import getFileObject # supplies fileObject

""" ___ Validator Functions ___
    Take string.
    Return an appropriate string if valid.
    Return None if invalid.
    Please read the feedModel module doc for further instructions."""
def isInteger(s):
    try:
        return int(s)
    except ValueError: return None

def coutryCodeAlpha2(s):
    """2 letters, all upper case"""
    if s == s.upper() and len(s) == 2:
        return '"' + s  + '"'
    return None

def coutryCodeAlpha3(s):
    """3 letters, all upper case"""
    if s == s.upper() and len(s) == 3:
        return '"' + s  + '"'
    return None

def countryTargetable(s):
    if s == '0' or s == '1':
        return s
    return None

def cityRegionID(s):
    if s == 'NULL':
        return s
    return isInteger(s)

def sqlString(s):
    return '"' + s  + '"'




""" Extend Feedclass for each feed to process """
class RouteFeed(CsvFeed):
    tableName = 'routes'
    validators = [isInteger, coutryCodeAlpha2, coutryCodeAlpha3, sqlString, countryTargetable]

class RegionFeed(CsvFeed):
    tableName = 'regions'
    validators = [isInteger, isInteger, sqlString, sqlString]

class CityFeed(Feed):
    tableName = 'cities'
    validators = [isInteger, sqlString, sqlString, isInteger, cityRegionID]
    firstLine = ['id', 'name', 'iso_code', 'country_id', 'region_id']
    def lineToList(self, line):
        d = json.loads(line) # The file contains dictionaries in json format.
        if not 'region_id' in d: d['region_id'] = 'NULL'
        return [d[key] for key in self.firstLine]




def main():
    db = MySQLdb.connect(host=settings.host, port=settings.port,
            user=settings.user, passwd=settings.passwd)
    cursor =  db.cursor()

    cursor.execute('use ' + settings.db_name)
    initialiseRemoteDB(cursor)

    RouteFeed().fetchFromFile(cursor, getFileObject('routes.gz'))
    RegionFeed().fetchFromFile(cursor, getFileObject('regions.csv'))
    CityFeed().fetchFromFile(cursor, getFileObject('cities.gz'))


if __name__ == '__main__':
    main()

