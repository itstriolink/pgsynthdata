import argparse
import os
import subprocess
import sys
from subprocess import Popen

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import postgres
from data_generator import DataGenerator

__version__ = '1.0'
examples = '''How to use pgsynthdata.py:

  python pgsynthdata.py test postgres -show
  \t-> Connects to database "test", host="localhost", port="5432", default user with password "postgres"
  \t-> Shows statistics from the tables in database test
  
  python pgsynthdata.py db pw1234 -H myHost -p 8070 -U testuser -show
  \t-> Connects to database "db", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Shows statistics from the tables in database db
  
  python pgsynthdata.py dbin dbgen pw1234 -H myHost -p 8070 -U testuser -generate
  \t-> Connects to database "dbin", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Generates synthetic data into "dbgen"
  
  python pgsynthdata.py dbin dbgen pw1234 -H myHost -p 8070 -U testuser -generate -tables table1, table2
  \t-> Connects to database "dbin", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Creates new database "dbgen" with synthetic data on tables: "table1" and "table2"
  
  python pgsynthdata.py --version
  \t-> Show the version of this program and quit'''

DUMP_FILE_PATH = 'schema.dump'
data_generator = DataGenerator()


def main():
    args = parse_arguments()

    if args.show:
        show(args)
    else:
        if args.DBNAMEGEN is None:
            sys.exit('When "-generate" argument is given, the following argument is required: DBNAMEGEN')
        else:
            connection = None
            try:

                connection = psycopg2.connect(dbname=args.DBNAMEIN,
                                              user=args.user,
                                              host=args.hostname,
                                              port=args.port,
                                              password=args.password)

                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = connection.cursor()

                generate(connection, cursor, args)
            except psycopg2.DatabaseError:
                sys.exit('''Connection failed because of at least one of the following reasons:
                        Database does not exist
                        User does not exist
                        Wrong password''')
            finally:
                if connection is not None:
                    connection.close()


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

    parser.add_argument('-mf', '--mf', type=float, default=1.0,
                        help='Multiplication factor (mf) for the generated synthesized data (default: 1.0)')
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
        connection = psycopg2.connect(dbname=args.DBNAMEIN,
                                      user=args.user,
                                      host=args.hostname,
                                      port=args.port,
                                      password=args.password)

        cursor = connection.cursor()

        postgres.show_database_stats(cursor)
        cursor.close()

    except psycopg2.DatabaseError:
        sys.exit('''Connection failed because of at least one of the following reasons:
                    Database does not exist
                    User does not exist
                    Wrong password''')
    finally:
        if connection is not None:
            connection.close()


def generate(connection, cursor, args):
    postgres.create_database(connection, cursor, args.DBNAMEGEN, args.owner)
    copy_database_structure(args)

    # postgres.analyze_database(cursor, args.DBNAMEIN)

    data_generator.generate(args)

    cursor.close()


def copy_database_structure(args):
    print(f'Copying the "{args.DBNAMEGEN}" database structure...')

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
