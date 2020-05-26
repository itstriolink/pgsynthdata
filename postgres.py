import sys

import psycopg2
from psycopg2 import sql


class DataTypes:
    VARCHAR_TYPES = [
        'text',
        'character varying',
        'character'
    ]

    NUMERIC_TYPES = [
        'smallint',
        'integer',
        'bigint',
        'decimal',
        'numeric'
    ]

    DATE_TYPES = [
        'date',
        'timestamp',
        'timestamp without time zone'
    ]

    BOOLEAN_TYPES = [
        'boolean',
        'bool'
    ]

    SUPPORTED_TYPES = VARCHAR_TYPES + NUMERIC_TYPES + DATE_TYPES + BOOLEAN_TYPES


def create_database(connection, cursor, db_name, owner_name):
    print(f'Creating the "{db_name}" database...')

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
            sys.exit('Database "{0}" could not be created. Error: {1}'.format(db_name, error))
    else:
        sys.exit(f'The database you tried to create "{db_name}" already exists. Please specify a new database name.')


def truncate_tables(connection, cursor):
    tables = get_tables(cursor)

    for table_info in tables:
        table_name = table_info[1]

        try:
            cursor.execute(f"""TRUNCATE TABLE {table_name};""")
        except psycopg2.DatabaseError as error:
            sys.exit('Could not truncate table "{0}". Error description: {1}'.format(table_name, error))

        connection.commit()


def show_database_stats(cursor):
    tables = get_tables(cursor)
    print(tables)

    for table_info in tables:
        table_name = table_info[1]
        sub_result = get_table_stats(cursor, table_name)

        print(f'\n -- {table_name} -- \n')
        print(sub_result)


def get_tables(cursor):
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
    except psycopg2.DatabaseError as error:
        sys.exit('Could not retrieve the database\'s table information. Error description: {0}'.format(error))


def get_table_stats(cursor, table_name):
    try:
        cursor.execute(f"""
           select attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation 
           from pg_stats 
           where schemaname not in ('pg_catalog') and tablename = '{table_name}'""")

        return cursor.fetchall()
    except psycopg2.DatabaseError as error:
        sys.exit('Could not get statistics for the "{0}" table. Error description: {1}'.format(table_name, error))


def get_column_information(cursor, table_name):
    try:
        cursor.execute(f"""
            SELECT 
                column_name, data_type, character_maximum_length,
                column_default,
                numeric_precision, numeric_precision_radix, numeric_scale
            FROM   information_schema.columns
            WHERE  table_name = '{table_name}'
            ORDER  BY ordinal_position;
            """)

        return cursor.fetchall()
    except psycopg2.DatabaseError as error:
        sys.exit('Could not get columns for the "{0}" table. Error description: {1}'.format(table_name, error))


def get_table_primary_keys(cursor, table_name):
    try:
        cursor.execute(f"""
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                         AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '{table_name}'::regclass
                    AND    i.indisprimary;""")

        return [r[0] for r in cursor.fetchall()]
    except psycopg2.DatabaseError as error:
        sys.exit(
            'Could not get the primary key information for the"{0}" table. Error description: {1}'.format(table_name,
                                                                                                          error))
