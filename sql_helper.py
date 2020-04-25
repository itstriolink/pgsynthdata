import sys

import psycopg2
from psycopg2 import sql


def db_connect(dbname, user, host, port, password):
    try:
        return psycopg2.connect(dbname=dbname,
                                user=user,
                                host=host,
                                port=port,
                                password=password)
    except psycopg2.DatabaseError:
        sys.exit('Could not connect to the database.')


def show_database_stats(cursor):
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
        sys.exit('Could not retrieve the database\'s table information')

    result = cursor.fetchall()
    print(result)
    for entry in result:
        table_name = entry[1]
        try:

            cursor.execute(f"""
        select attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation 
        from pg_stats 
        where schemaname not in ('pg_catalog') and tablename = '{table_name}'""")
        except psycopg2.DatabaseError:
            sys.exit(f'Could not get statistics for the "{table_name}" table')

        sub_result = cursor.fetchall()
        print(f'\n -- {table_name} -- \n')
        print(sub_result)


def create_database(connection, cursor, db_name, owner_name):
    print(f'Creating {db_name} database...')

    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()

    if not exists:
        try:
            if owner_name is None:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(db_name)))
            else:
                cursor.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(db_name),
                    sql.Identifier(owner_name)))

            connection.commit()
        except psycopg2.DatabaseError as error:
            sys.exit('Database could not be created. Error: {}'.format(error))
    else:
        sys.exit(f'The database you tried to create "{db_name}" already exists. Please specify a new database name.')
