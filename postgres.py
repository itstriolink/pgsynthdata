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

        except psycopg2.DatabaseError as error:
            sys.exit('Database could not be created. Error: {}'.format(error))
    else:
        sys.exit(f'The database you tried to create "{db_name}" already exists. Please specify a new database name.')


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
        sub_result = get_table_stats(cursor, table_name)

        print(f'\n -- {table_name} -- \n')
        print(sub_result)


def get_table_stats(cursor, table_name):
    try:
        cursor.execute(f"""
           select attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation 
           from pg_stats 
           where schemaname not in ('pg_catalog') and tablename = '{table_name}'""")

        return cursor.fetchall()
    except psycopg2.DatabaseError:
        sys.exit(f'Could not get statistics for the "{table_name}" table')


def get_table_information(cursor, generated=False):
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

        return cursor.fetchall()
    except psycopg2.DatabaseError:
        sys.exit('Could not get table information for the {0}database'.format("generated " if generated else ""))


def get_column_information(cursor, table_name, generated=False):
    try:
        cursor.execute(f"""
            SELECT 
                column_name, data_type, character_maximum_length
            FROM   information_schema.columns
            WHERE  table_name = '{table_name}'
            ORDER  BY ordinal_position;
            """)

        return cursor.fetchall()
    except psycopg2.DatabaseError:
        sys.exit('Could not get columns for the {0}"{1}" table'.format("generated " if generated else "", table_name))


def get_table_primary_key(cursor, table_name, generated=False):
    try:
        cursor.execute(f"""
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                         AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '{table_name}'::regclass
                    AND    i.indisprimary;""")

        return cursor.fetchone()
    except psycopg2.DatabaseError:
        sys.exit(
            'Could not get the primary key information for the {0}"{1}" table'.format("generated " if generated else "",
                                                                                      table_name))
