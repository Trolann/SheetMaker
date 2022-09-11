import sqlite3
from time import sleep, time
from ratelimiter import RateLimiter
from os import environ
from random import uniform
from pathlib import Path

global dev_instance
global path

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

path = environ['DIR_PATH']

try:
    dealcatcherdb_path = environ['DB_DIR']
    print('Dealcatcher DB path: {}'.format(path))
except KeyError:
    dealcatcherdb_path = ''
    print('SheetMaker DB path: local'.format(path))

shared_db = Path(f"{dealcatcherdb_path}dealcatcher.db")

if not shared_db.is_file():
    print('There is no DB loaded')
    

def _db_recur(cursor, sql, called_from, recur_depth = 0):
    try:
        cursor.execute(sql)
    except sqlite3.OperationalError as e:
        if recur_depth <= 5:
            sleep(uniform(0.5, 4.5))
            recur_depth += 1
            print('Recur DB called for {}. Depth: {}'.format(called_from, recur_depth))
            _db_recur(cursor, sql, called_from, recur_depth = recur_depth)
        else:
            print('recur failed. SQL NOT EXECUTED:')
            print(f'{sql}')
            print(f'called from: {called_from}')
            print(e)


def remove(db, table, option, get_dev = False, commit_to_db=True):
    global dev_instance
    if dev_instance and get_dev:
        option = 'dev_' + option
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    if dev_instance and get_dev:
        sql = "DELETE FROM {} WHERE option = '{}'".format(table, option)
    else:
        sql = "DELETE FROM {} WHERE option = '{}' AND option NOT LIKE \'dev_%\'".format(table, option)
    _db_recur(cursor, sql, 'remove')
    if commit_to_db:
        connection.commit()
    cursor.close()


def remove_like_value(db, table, option_like, value_like, get_dev = False,  commit_to_db=True):
    global dev_instance
    if dev_instance and get_dev:
        option_like = 'dev_' + option_like
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    if dev_instance and get_dev:
        sql = "DELETE FROM {} WHERE option = '{}' AND value LIKE '%{}%'".format(table, option_like, value_like)
    else:
        sql = "DELETE FROM {} WHERE option = '{}' AND option NOT LIKE \'dev_%\' AND value LIKE '%{}%'".format(table, option_like, value_like)
    _db_recur(cursor, sql, 'remove_like')
    if commit_to_db:
        connection.commit()
    cursor.close()


def insert(db, table, option, value, get_dev = False, commit_to_db = True):
    """Inserts into sqlite db with the option:value schema
        Recursively calls _insert wrapper to avoid collisions/locked db"""
    global dev_instance

    if dev_instance and get_dev:
        option = 'dev_' + option

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    to_insert = (option, value)  # Tuple insertion avoids injections
    sql = 'INSERT OR REPLACE INTO {} VALUES {}'.format(table, to_insert)
    _db_recur(cursor, sql, 'insert')  # Wrapper for recursion

    if commit_to_db:
        connection.commit()

    # if dev_instance:
    #     print('Value inserted into {} option {}: {}'.format(table, option, value))
    cursor.close()
    connection.close()


def get_value(db, table, option, get_dev = False):
    global dev_instance

    if dev_instance and get_dev:
        option = 'dev_' + option

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    if dev_instance and get_dev:
        sql = 'SELECT value FROM {} WHERE option LIKE \'%{}%\''.format(table, option)
    else:
        sql = 'SELECT value FROM {} WHERE option LIKE \'%{}%\' AND option NOT LIKE \'%dev_%\''.format(table, option)
    _db_recur(cursor, sql, 'get_value')
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return_val = rows[0][0] if rows else 0
    if dev_instance:
        print('Get value result for {}: {}'.format(option, return_val))
    return return_val


def select_from_table(db, table, option, get_dev = False):
    global dev_instance

    if dev_instance and get_dev:
        option = 'dev_' + option

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    if dev_instance and get_dev:
        sql = 'SELECT value FROM {} WHERE option LIKE \'%{}%\''.format(table, option)
    else:
        sql = 'SELECT value FROM {} WHERE option LIKE \'%{}%\' AND option NOT LIKE \'dev_%\''.format(table, option)
    _db_recur(cursor, sql, 'select_from_table')
    rows = cursor.fetchall()
    values = []
    for _value in rows:
        values.append(_value[0])
    cursor.close()
    connection.close()
    if dev_instance and 'After' in values:
        print('Get table length of {}: {}'.format(option, values))
    return values


class ConfigDB:
    def __init__(self):
        global path
        self.environ_path = path
        self._configdb = path + 'config.db'
        self.table = 'config'
        self.heartbeat_url = self.get_heartbeat('url')
        self.timer_expire = 0
        self.api_calls = 0
        self.rate_limiter = RateLimiter(max_calls=30, period=60, callback=self.limited)

    def limited(self, until):
        duration = int(round(until - time()))
        if duration > 2:
            print(f'Rate limited, sleeping for {duration:d} seconds')
        else:
            print('Rate limited, sleeping for 1 second.')

    def get_heartbeat(self, value):
        value = 'heartbeat_' + value
        return get_value(self._configdb, self.table, value, get_dev = True)

    def get_all_values(self, sheet):
        with self.rate_limiter:
            return sheet.get_all_values()

    def new_deal(self, sheet, current_deal):
        with self.rate_limiter:
            sheet.append_row(current_deal, values_input_option='USER_ENTERED')

    def get_worksheet(self, deal_sheet, vendor_name):
        with self.rate_limiter:
            return deal_sheet.worksheet(vendor_name)

    def remove_deal(self, sheet, deal_name):
        row = sheet.find(deal_name).row
        with self.rate_limiter:
            sheet.delete_rows(row)


config_db = ConfigDB()
