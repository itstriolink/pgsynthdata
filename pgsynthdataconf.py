import sys

import argparse
from pathlib import Path

import psycopg2

examples = '''How to use pgsynthdataconf.py:

  python pgsynthdataconf.py test postgres
  \t-> Connects to database "test", host="localhost", port="5432", default user with password "postgres"
  \t-> Creates a file named "test.conf"
  
  python pgsynthdataconf.py db pw1234 config.conf -H myHost -P 8070 -U testuser
  \t-> Connects to database "db", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Creates a file named "config.conf"'''


def main():
    args = parse_arguments()
    database = args.database
    db_name = f'dbname={database}'
    host = '' if args.hostname is None else f' host={args.hostname}'
    port = '' if args.port is None else f' port={args.port}'
    user = '' if args.user is None else f' user={args.user}'
    password = f' password={args.password}'
    parameters = f'{db_name}{host}{port}{user}{password}'
    connect(parameters)

    config_file = Path(f'{database}.conf') if args.config is None else Path(args.config)
    write_file(config_file)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Connects to a database and reads statistics', epilog=examples,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('database', type=str, help='An existing postgres database')
    parser.add_argument('-H', '--hostname', type=str, help='Specifies the host name')
    parser.add_argument('-P', '--port', type=int, help='Specifies the TCP/IP port')
    parser.add_argument('-U', '--user', type=str, help='An existing postgres database user')
    parser.add_argument('-V', '--version', type=str, help='Version information')
    parser.add_argument('password', type=str, help='Required user password')
    parser.add_argument('config', type=str, nargs='?', help='Name of the config file to be created (default: same as '
                                                            'database name)')
    return parser.parse_args()


def connect(parameters):
    connection = None
    try:
        connection = psycopg2.connect(parameters)
        cursor = connection.cursor()
        cursor.execute("select * from pg_stats where tablename = 'actor' and attname = 'first_name';")
        result = cursor.fetchall()
        cursor.close()
    except psycopg2.DatabaseError:
        sys.stdout.write('''Connection failed because of at least one of the following reasons:
    Could not find specified database or database does not exist
    User does not exist
    Wrong password''')
    finally:
        if connection is not None:
            connection.close()


def write_file(config_file):
    with open(config_file, 'w') as w:
        w.write('todo')


if __name__ == '__main__':
    main()
