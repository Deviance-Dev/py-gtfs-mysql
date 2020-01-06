#!/bin/python

from datetime import datetime
import csv
import MySQLdb
import settings

def is_numeric(s):
    try:
      i = float(s)
    except ValueError:
        # not numeric
        return False
    else:
        # numeric
        return True

def main():
    db = MySQLdb.connect (host=settings.MYSQL_HOST, user=settings.MYSQL_USER, passwd=settings.MYSQL_PASSWORD, db=settings.MYSQL_DATABASE, use_unicode=True, charset='utf8mb4', init_command='SET NAMES utf8mb4;')
    cursor = db.cursor()

    # Disable MySQL slow queries
    cursor.execute("SET autocommit=0;")
    cursor.execute("START TRANSACTION;")

    TABLES = ['agency', 'calendar', 'calendar_dates', 'fare_attributes', 'fare_rules', 'feed_info', 'frequencies', 'operators', 'operator_routes', 'routes', 'shapes', 'stops', 'stop_times', 'trips']
    print('START %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    for table in TABLES:
        print('processing %s %s' %(table, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        cursor.execute('TRUNCATE TABLE %s' % table)
        f = open('gtfs/%s.txt' % table, 'r')
        reader = csv.reader(f)
        columns = reader.next()
        for row in reader:
            insert_row = []
            for value in row:
                if not is_numeric(value):
                    insert_row.append('"' + MySQLdb.escape_string(value) + '"')
                else:
                    insert_row.append(value)
            insert_sql = "INSERT INTO %s (%s) VALUES (%s);" % (table, ','.join(columns), ','.join(insert_row))        
            # Execute the SQL
            cursor.execute(insert_sql)
    # Commit changes in the database
    db.commit()
    # disconnect from server
    db.close()
    print('END %s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()
