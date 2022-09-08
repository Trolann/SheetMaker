import sqlite3
from time import sleep
from os import environ
from random import uniform

global dev_instance
global path
global heartbeat_url

try:
    dev_instance = bool(int(environ['DEV_INSTANCE']))
    if dev_instance:
        print('-' * 35)
        print('DEVELOPMENT INSTANCE   ' * 3)
        print('-' * 35)
    else:
        print('-' * 35)
        print('**PROD' * 10 + '**')
        print('**PROD' * 10 + '**')
        print('**PROD' * 10 + '**')
        print('-' * 35)
except KeyError:
    print('-' * 35)
    print('DEVELOPMENT INSTANCE   ' * 3)
    print('-' * 35)
    dev_instance = True

try:
    path = environ['DIR_PATH']
    print(f'Dealcatcher path: {path}')
except KeyError:
    path = ''
    print('Dealcatcher path: local')


try:
    dc_path = environ['DB_DIR']
    print(f'DealCatcher DB path: {dc_path}')
except KeyError:
    dc_path = ''
    print('DealCatcher DB path: local')

try:
    heartbeat_url = environ['HEARTBEAT_URL']
    print(f'DealCatcher heartbeat url: {heartbeat_url}')
except KeyError:
    heartbeat_url = ''
    print('NO HEARTBEAT URL LOADED')


def _db_recur(cursor, sql, called_from, recur_depth = 0):
    try:
        cursor.execute(sql)
    except sqlite3.OperationalError:
        if recur_depth <= 5:
            sleep(uniform(0.5, 7.5))  # Prevent multiple branches from colliding multiple times
            recur_depth += 1
            print('Recur DB called for {}. Depth: {}'.format(called_from, recur_depth))
            _db_recur(cursor, sql, called_from, recur_depth = recur_depth)
        else:
            print('recur failed. SQL NOT EXECUTED:')
            print(f'{sql}')
            print(f'called from: {called_from}')


def remove(db, table, option, commit_to_db=True):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = "DELETE FROM {} WHERE url = '{}'".format(table, option)
    _db_recur(cursor, sql, 'remove')
    if commit_to_db:
        connection.commit()
    cursor.close()


def remove_like_value(db, table, vendor_like, url_like, commit_to_db=True):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = "DELETE FROM {} WHERE vendor = '{}' AND url LIKE '%{}%'".format(table, vendor_like, url_like)
    _db_recur(cursor, sql, 'remove_like_value')
    if commit_to_db:
        connection.commit()
    cursor.close()


def insert(db, table, to_insert, commit_to_db = True):
    """Inserts into sqlite db with the option:value schema"""
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = 'INSERT OR REPLACE INTO {} VALUES {}'.format(table, to_insert)
    _db_recur(cursor, sql, 'insert')

    if commit_to_db:
        connection.commit()

    cursor.close()
    connection.close()


def _get_value(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except:
        sleep(1)
        _get_value(cursor, sql)


def get_value(db, table, option):

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = 'SELECT value FROM {} WHERE option LIKE \'%{}%\''.format(table, option)
    rows = _get_value(cursor, sql)
    cursor.close()
    connection.close()
    return_val = rows[0][0] if rows else 0

    return return_val


def _table_select(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except:
        sleep(1)
        _table_select(cursor, sql)


def select_from_table(db, table, column, option):

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    sql = 'SELECT * FROM {} WHERE {} LIKE \'%{}%\''.format(table, column, option)

    rows = _table_select(cursor, sql)
    cursor.close()
    connection.close()

    return rows


class DealCatcherDB:
    def __init__(self):
        global dc_path
        global path
        global heartbeat_url
        self._dealcatcherdb = dc_path + 'dealcatcher.db'
        self.vendors_table = 'vendors'
        self.active_table = 'active_deals'
        self.dir_path = path
        self.heartbeat_url = heartbeat_url

    def get_vendors(self):
        vendor_list = select_from_table(self._dealcatcherdb, self.vendors_table, 'website', 'http')
        return vendor_list

    def new_deal(self, vendor_acronym, deal_tuple):
        name, url, image_url, amount, in_stock, description = deal_tuple
        insert_tuple = (vendor_acronym, name, url, image_url, amount, in_stock, description)
        insert(self._dealcatcherdb, self.active_table, insert_tuple)  # Add the new record

    def expired_deal(self, vendor_acronym, deal_tuple):
        name, url, image_url, amount, in_stock, description = deal_tuple
        remove_like_value(self._dealcatcherdb, self.active_table, vendor_acronym, url)

    def get_deals(self, vendor_acronym = None):
        if vendor_acronym:  # This is a call to show all deals for this vendor
            rows = select_from_table(self._dealcatcherdb, self.active_table, 'vendor', vendor_acronym)

        else:  # This is a search request, return every deal in existence
            rows = list()
            for acronym, name, website, thumbnail in self.get_vendors():
                vendor_rows = select_from_table(self._dealcatcherdb, self.active_table, 'vendor', acronym)
                for row in vendor_rows:
                    print(row)
                    rows.append(row)

        return rows


dealcatcher_db = DealCatcherDB()

