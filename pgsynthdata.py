import argparse
import psycopg2


class DatabaseMissingError(Exception):
    """Raised when '-generate' is given, but not 'DBNAMEGEN'"""


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


def main():
    args = parse_arguments()

    if args.show:
        database = args.DBNAMEIN
        db_name = f'dbname={database}'
        host = '' if args.hostname is None else f' host={args.hostname}'
        port = '' if args.port is None else f' port={args.port}'
        user = '' if args.user is None else f' user={args.user}'
        password = f' password={args.password}'
        parameters = f'{db_name}{host}{port}{user}{password}'
        connect(parameters)
    else:
        if args.DBNAMEGEN is None:
            raise DatabaseMissingError('When "-generate" is given, the following argument is required: DBNAMEGEN')
        else:
            print('todo')


def parse_arguments():
    parser = argparse.ArgumentParser(description='Connects to a database and reads statistics', epilog=examples,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version=f'pgsynthdata version: {__version__}')
    parser.add_argument('DBNAMEIN', type=str, help='Name of an existing postgres database')
    parser.add_argument('DBNAMEGEN', type=str, nargs='?', help='Name of database to be created')  # optional, but not if DBNAMEGEN is given
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


def connect(parameters):
    connection = None
    try:
        connection = psycopg2.connect(parameters)
        cursor = connection.cursor()
        cursor.execute("""
    SELECT 
        nspname AS schemaname,relname as tablename, reltuples::bigint as rowcount,rank() over(order by reltuples desc)
    FROM pg_class C
    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE 
        nspname NOT IN ('pg_catalog', 'information_schema') AND
        relkind='r' 
    ORDER BY tablename;""")
        result = cursor.fetchall()
        print(result)
        for entry in result:
            table_name = entry[1]
            cursor.execute(f"""
    select attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation 
    from pg_stats 
    where schemaname not in ('pg_catalog') and tablename = '{table_name}'""")
            sub_result = cursor.fetchall()
            print(f'\n -- {table_name} -- \n')
            print(sub_result)
        cursor.close()
    except psycopg2.DatabaseError:
        print('''Connection failed because of at least one of the following reasons:
        Could not find specified database or database does not exist
        User does not exist
        Wrong password''')
    finally:
        if connection is not None:
            connection.close()


if __name__ == '__main__':
    main()
