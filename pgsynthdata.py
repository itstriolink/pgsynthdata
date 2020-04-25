import argparse
import datetime
import os
import random
import subprocess
import sys
from subprocess import Popen

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import random_helper
import sql_helper


class DatabaseError(Exception):
    """Raised when any error happens in the database"""


tables_and_columns = dict()

__version__ = '1.0'
examples = '''How to use pgsynthdata.py:

  python pgsynthdata.py test postgres -show
  \t-> Connects to database "test", host="localhost", port="5432", default user with password "postgres"
  \t-> Shows statistics from certain tables in database test
  
  python pgsynthdata.py db pw1234 -H myHost -p 8070 -U testuser -show
  \t-> Connects to database "db", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Shows statistics from certain tables in database db
  
  python pgsynthdata.py dbin dbgen pw1234 -H myHost -p 8070 -U testuser -generate
  \t-> Connects to database "dbin", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Creates new database "dbgen" with synthesized data
  
  python pgsynthdata.py --version
  \t-> Show the version of this program and quit'''

DUMP_FILE_PATH = 'schema.dump'


def main():
    args = parse_arguments()

    database = args.DBNAMEIN
    db_name = f'dbname=postgres' if database is None else f'dbname={database}'

    host = '' if args.hostname is None else f' host={args.hostname}'
    port = '' if args.port is None else f' port={args.port}'
    user = '' if args.user is None else f' user={args.user}'

    password = f' password={args.password}'
    parameters = f'{db_name}{host}{port}{user}{password}'

    if args.show:
        connect_show(args)
    else:
        if args.DBNAMEGEN is None:
            sys.exit('When "-generate" argument is given, the following argument is required: DBNAMEGEN')
        else:
            connect_gen(args, args.DBNAMEIN, args.DBNAMEGEN, args.owner)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Connects to a database and reads statistics', epilog=examples,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version=f'pgsynthdata version: {__version__}')
    parser.add_argument('DBNAMEIN', type=str, help='Name of an existing postgres database')
    parser.add_argument('DBNAMEGEN', type=str, nargs='?',
                        help='Name of database to be created')  # optional, but not if DBNAMEGEN is given
    parser.add_argument('password', type=str, help='Required user password')

    # One of the two options in action_group has to be given, but not both
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('-show', '--show', action='store_true', help='If given, shows config')
    action_group.add_argument('-generate', '--generate', action='store_true',
                              help='If given, generates new synthesized data to database DBNAMEGEN')

    parser.add_argument('-O', '--owner', type=str, help='Owner of the database, default: same as user')
    parser.add_argument('-H', '--hostname', type=str, help='Specifies the host name, default: localhost')
    parser.add_argument('-p', '--port', type=int, help='Specifies the TCP/IP port, default: 5432')
    parser.add_argument('-U', '--user', type=str, help='An existing postgres database user, default: default user')

    return parser.parse_args()


def connect_show(args):
    connection = None
    try:
        connection = sql_helper.db_connect(args.DBNAMEIN, args.user, args.hostname, args.port, args.password)
    except psycopg2.DatabaseError:
        sys.exit('''Connection failed because of at least one of the following reasons:
            Database does not exist
            User does not exist
            Wrong password''')

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    sql_helper.show_database_stats(cursor)
    cursor.close()


def connect_gen(args, db_name_in, db_name_gen, owner_name=None):
    connection = None
    try:
        connection = sql_helper.db_connect(db_name_in, args.user, args.hostname, args.port, args.password)
    except psycopg2.DatabaseError:
        sys.exit('''Connection failed because of at least one of the following reasons:
            Database does not exist
            User does not exist
            Wrong password''')

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    sql_helper.create_database(connection, cursor, db_name_gen, owner_name)

    try:
        connection = sql_helper.db_connect(db_name_gen, args.user, args.hostname, args.port, args.password)
    except psycopg2.DatabaseError:
        sys.exit(f"Could not connect to the newly created database: {args.DBNAMEGEN}")

    cursor = connection.cursor()

    copy_database_structure(args)

    try:
        cursor.execute("""
        SELECT 
            nspname AS schemaname,relname as tablename, reltuples::bigint as rowcount,rank() over(order by reltuples desc)
        FROM pg_class C
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
        WHERE 
            nspname NOT IN ('pg_catalog', 'information_schema') AND
            relkind='r' 
        ORDER BY tablename;""")
    except psycopg2.DatabaseError:
        sys.exit('Could not retrieve the generated database\'s table information')

    table_results = cursor.fetchall()

    for table_entry in table_results:
        table_name = table_entry[1]
        cursor.execute(f"""
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                 AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{table_name}'::regclass
            AND    i.indisprimary;""")

        primary_column_result = cursor.fetchone()

        primary_column = None
        if primary_column_result:
            primary_column = primary_column_result[0]

        try:
            cursor.execute(f"""
                SELECT 
                    column_name, data_type, character_maximum_length
                FROM   information_schema.columns
                WHERE  table_name = '{table_name}'
                ORDER  BY ordinal_position;
                """)
        except psycopg2.DatabaseError:
            sys.exit(f'Could not get columns for the generated "{table_name}" table')

        column_results = cursor.fetchall()
        columns_tuple = list()

        for column_entry in column_results:
            if column_entry[0] != primary_column:
                columns_tuple.append((column_entry[0], column_entry[1], column_entry[2]))

        tables_and_columns[table_name] = columns_tuple

        columns_information = list()

        for column_info in tables_and_columns.get(table_name):
            columns_information.append(column_info[0])

        start_date = datetime.date(year=1950, month=1, day=1)
        end_date = datetime.date(year=2020, month=1, day=1)

        for lp in range(100):
            insert_query = "INSERT INTO {}("
            insert_query += '{0}{1}'.format(', '.join(columns_information), ') VALUES (')

            column_values = list()

            for column_info in tables_and_columns.get(table_name):
                if column_info[1] == 'integer' or column_info[1] == 'numeric':
                    column_values.append("{0}".format(random_helper.random_number(0, 1000)))
                elif column_info[1] == 'date':
                    column_values.append("'{0}'".format(random_helper.random_date(start_date, end_date)))
                else:
                    column_values.append("'{0}'".format(random_helper.random_word(
                        random.randrange(50 if column_info[2] is None else (column_info[2] + 1))
                    )))

            insert_query += '{0}{1}'.format(', '.join(column_values), ');')

            cursor.execute(
                sql.SQL(insert_query).format(
                    sql.Identifier(table_name)
                )
            )

    connection.commit()

    cursor.close()
    connection.close()


def copy_database_structure(args):
    print(f'Copying {args.DBNAMEGEN} database structure...')

    try:
        process = Popen(['pg_dump',
                         '--dbname=postgresql://{}:{}@{}:{}/{}'.format(args.user,
                                                                       args.password,
                                                                       'localhost',
                                                                       '5432',
                                                                       args.DBNAMEIN),
                         '-s',
                         '-Fc',
                         '-f', DUMP_FILE_PATH
                         ],
                        stdout=subprocess.PIPE)

        process.communicate()[0]

        process = Popen(['pg_restore',
                         '--dbname=postgresql://{}:{}@{}:{}/{}'.format(args.user,
                                                                       args.password,
                                                                       'localhost',
                                                                       '5432',
                                                                       args.DBNAMEGEN),
                         DUMP_FILE_PATH],
                        stdout=subprocess.PIPE
                        )

        process.communicate()[0]
    except Exception as error:
        sys.exit('Database structure could not be copied. Error: {}'.format(error))
    finally:
        if os.path.exists(DUMP_FILE_PATH):
            os.remove(DUMP_FILE_PATH)


if __name__ == '__main__':
    main()
