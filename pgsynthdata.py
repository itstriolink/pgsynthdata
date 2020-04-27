import argparse
import datetime
import os
import random
import subprocess
import sys
from subprocess import Popen
from typing import Dict

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import postgres
import utils


class DatabaseError(Exception):
    """Raised when any error happens in the database"""


START_DATE = datetime.date(year=1950, month=1, day=1)
END_DATE = datetime.date.today()

table_information: Dict = {}

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
        show(args)
    else:
        if args.DBNAMEGEN is None:
            sys.exit('When "-generate" argument is given, the following argument is required: DBNAMEGEN')
        else:
            generate(args, args.DBNAMEIN, args.DBNAMEGEN, args.owner)


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

    parser.add_argument('-r', '--recreate', action='store_true',
                        help="Recreate the DBNAMEGEN database with the same schema as DBNAMEIN.")
    parser.add_argument('-tables', '--tables', type=str,
                        help='Only generate data for specific tables, separated by a comma')

    parser.add_argument('-O', '--owner', type=str, help='Owner of the database, default: same as user')
    parser.add_argument('-H', '--hostname', type=str, help='Specifies the host name, default: localhost')
    parser.add_argument('-p', '--port', type=int, help='Specifies the TCP/IP port, default: 5432')
    parser.add_argument('-U', '--user', type=str, help='An existing postgres database user, default: default user')

    return parser.parse_args()


def show(args):
    connection = None
    try:
        connection = postgres.db_connect(args.DBNAMEIN, args.user, args.hostname, args.port, args.password)
    except psycopg2.DatabaseError:
        sys.exit('''Connection failed because of at least one of the following reasons:
            Database does not exist
            User does not exist
            Wrong password''')

    cursor = connection.cursor()

    postgres.show_database_stats(cursor)
    cursor.close()


def generate(args, db_name_in, db_name_gen, owner_name=None):
    connection = None
    try:
        connection = postgres.db_connect(args.DBNAMEIN, args.user, args.hostname, args.port, args.password)
    except psycopg2.DatabaseError:
        sys.exit('''Connection failed because of at least one of the following reasons:
                Database does not exist
                User does not exist
                Wrong password''')

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    if args.recreate:
        postgres.create_database(connection, cursor, db_name_gen, owner_name)
        copy_database_structure(args)
    else:
        connection = postgres.db_connect(db_name_gen, args.user, args.hostname, args.port, args.password)
        cursor = connection.cursor()

        postgres.truncate_tables(connection, cursor)

    connection = postgres.db_connect(db_name_in, args.user, args.hostname, args.port, args.password)

    table_results = postgres.get_table_information(cursor, generated=True)

    tables_list = None
    if args.tables is not None:
        tables_list = args.tables.split(",")
        tables_list = [table.strip(' ') for table in tables_list]

    insert_dict = dict()
    for table_entry in table_results:
        cursor = connection.cursor()
        table_name = table_entry[1]

        if tables_list is not None:
            if table_name not in tables_list:
                continue

        primary_column_result = postgres.get_table_primary_key(cursor, table_name, generated=True)
        primary_column = None

        if primary_column_result:
            primary_column = primary_column_result[0]

        column_results = postgres.get_column_information(cursor, table_name, generated=True)

        table_information[table_name] = {}
        table_information[table_name]["column_information"] = {}
        table_information[table_name]["stats"] = {}

        for column_entry in column_results:
            if column_entry[0] != primary_column:
                columns_dict = dict()
                columns_dict["column_name"] = column_entry[0]
                columns_dict["data_type"] = column_entry[1]
                columns_dict["max_length"] = column_entry[2]
                table_information[table_name]["column_information"][column_entry[0]] = columns_dict

        table_stats = postgres.get_table_stats(cursor, table_name)

        for stats_entry in table_stats:
            stats_dict = dict()
            stats_dict["column_name"] = stats_entry[0]
            stats_dict["null_frac"] = stats_entry[1]
            stats_dict["avg_width"] = stats_entry[2]
            stats_dict["n_distinct"] = stats_entry[3]
            stats_dict["most_common_vals"] = stats_entry[4]
            stats_dict["most_common_freqs"] = stats_entry[5]
            stats_dict["histogram_bounds"] = stats_entry[6]
            stats_dict["correlation"] = stats_entry[7]
            table_information[table_name]["stats"][stats_entry[0]] = stats_dict

        column_names = list()

        for column_name in table_information.get(table_name)["column_information"].keys():
            column_names.append(column_name)

        insert_query = ""
        for _ in range(100):
            insert_query += "INSERT INTO {table_name}("
            insert_query += '{0}{1}'.format(', '.join(column_names), ') VALUES (')

            column_values = list()

            for column_info in table_information[table_name]["column_information"].values():
                data_type = column_info.get("data_type")
                max_length = column_info.get("max_length")

                if data_type == 'integer' or data_type == 'numeric':
                    column_values.append("{0}".format(utils.random_number(0, 1000)))
                elif data_type == 'date':
                    column_values.append("'{0}'".format(utils.random_date(START_DATE, END_DATE)))
                else:
                    column_values.append("'{0}'".format(utils.random_word(
                        random.randrange(50 if max_length is None else (max_length + 1))
                    )))

            insert_query += '{0}{1}'.format(', '.join(column_values), ');')

        insert_dict[table_name] = insert_query

    connection = postgres.db_connect(db_name_gen, args.user, args.hostname, args.port, args.password)

    cursor = connection.cursor()
    for table_name, insert_query in insert_dict.items():
        cursor.execute(
            sql.SQL(insert_query).format(
                table_name=sql.Identifier(table_name)
            )
        )
        connection.commit()

    sys.stdout.write(f"Successfully generated the synthetic data into the \"{db_name_gen}\" database.")

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
