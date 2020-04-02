import sys

import psycopg2
from psycopg2 import sql

import argparse
from pathlib import Path


examples = '''How to use pgsynthdataconf.py:

  python pgsynthdataconf.py test postgres
  \t-> Connects to database "test", host="localhost", port="5432", default user with password "postgres"
  \t-> Creates a file named "test.conf"

  python pgsynthdataconf.py db pw1234 config.conf -H myHost -P 8070 -U testuser
  \t-> Connects to database "db", host="myHost", port="8070", user="testuser" with password "pw1234"
  \t-> Creates a file named "config.conf"'''


def main():
    args = parse_arguments()
    db_name = args.database
    host = '' if args.hostname is None else f' host={args.hostname}'
    port = '' if args.port is None else f' port={args.port}'
    user = '' if args.user is None else f' user={args.user}'
    password = f' password={args.password}'
    owner = args.owner
    config_file_path = args.config
    parameters = f'dbname=postgres{host}{port}{user}{password}'
    connect(parameters, db_name, owner, config_file_path)

    # config_file = Path(args.config)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Reads a config file and generates synthetic data on a new database',
                                     epilog=examples,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)
    parser.add_argument('database', type=str, help='Name of the database to be created')
    parser.add_argument('-H', '--hostname', type=str, help='Specifies the host name')
    parser.add_argument('-P', '--port', type=int, help='Specifies the TCP/IP port')
    parser.add_argument('-U', '--user', type=str, help='An existing postgres database user')
    parser.add_argument('-O', '--owner', type=str, help='Owner of new database')
    parser.add_argument('password', type=str, help='Required user password')
    parser.add_argument('config', type=str, help='Name of the config file to read from')
    parser.add_argument('-V', '--version', type=str, help='Version information')
    parser.add_argument('-?', '--help', type=str, help='Show help')

    return parser.parse_args()


def connect(parameters, db_name, owner, config_file_path):
    connection = None
    try:
        connection = psycopg2.connect(parameters)
        connection.autocommit = True

        cursor = connection.cursor()
        create_database(cursor, db_name, owner)
        cursor.close()
    except psycopg2.DatabaseError:
        sys.stdout.write('''Connection failed because of at least one of the following reasons:
    User does not exist
    Wrong password''')
    finally:
        if connection is not None:
            connection.close()


def create_database(cursor, db_name, owner):
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()
    create_db_statement = 'CREATE DATABASE {db_name};' if owner is None else 'CREATE DATABASE {db_name} OWNER {owner};'
    if not exists:
        cursor.execute(sql.SQL(create_db_statement).format(
            db_name=sql.Identifier(db_name),
            owner=sql.Identifier(owner) if owner is not None else None))
    else:
        sys.stdout.write('The database you tried to create already exists. Please specify a new database name.')


if __name__ == '__main__':
    main()
