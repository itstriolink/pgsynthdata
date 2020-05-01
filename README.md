# Pure Synthetic Data Generation with a PostgreSQL and Python-based Tool.

Description
------
A lightweight tool written in Python that teams up with PostgreSQL in order to generate fully synthetic data that seem as realistic as possible.

The *Tennis_ATP* dataset can be found inside resources/ and can be set-up very easily using the *import.bat* file (if on Windows) or importing the *.csv* files directly into Postgres (which should be pretty straight-forward).

Usage
------
Tool arguments:
*  **DBNAMEGEN** - Name of the database to be created
*  **-show/--show** - Shows database stats (default)
*  **-generate/--generate** - Generates new synthesized data to database DBNAMEGEN
*  **-mf/--mf** - Multiplication factor for the generated synthetic data (default: 1.0)
*  **-tables/--tables** - Name(s) of table(s) to be filled, separated with ',', ignoring other tables (default: fill all tables)
*  **-r/--recreate** - (Re-)create new DBNAMEGEN and schema (default: don't recreate database/schema, just truncate the tables)
*  **-O/--owner** - Owner of new database (default: same as user)
*  **-v/--version** - Show version information, then quit
*  **-h/--help** - Show tool help, then quit


Connection options:
*  **DBNAMEIN** - Name of the existing database to connect to
*  **-H/--hostname** - Name of the PostgreSQL server (default: localhost)
*  **-P/--port** - Port of the PostgreSQL server (default: 5432)
*  **-U/--user** - PostgreSQL server username


Some usage examples:
*  **python pgsynthdata.py test postgres -show**
   * Connects to database *test*, host=*localhost*, port=*5432*, default user with
password *postgres*
   * Shows statistics of the database *test*
* **python pgsynthdata.py dbin dbgen pw1234 -H myHost -p 8070 -U testuser -generate**
  * Connects to database *dbin*, host=*myHost*, port=*8070*, user=*testuser* with
password *pw1234*
  * Truncates tables of *dbgen* and generates synthetic data into them
* **python pgsynthdata.py dbin dbgencreate pw123 -U myUser -generate -r**
  * Connects to database *dbin*, host=*localhost*, port=*5432*, user=*myUser* with password
*pw123*
  * Creates new database *dbgencreate* with the same schema as *dbin* and
generates synthetic data into it
* **python pgsynthdata.py dbin dbgencreate pw123 -U myUser -generate -tables myTable1, myTable2**
  * Connects to database *dbin*, host=*localhost*, port=*5432*, user=*myUser* with
password *pw123*
  * Only truncates the *myTable1* and *myTable2* tables and generates synthetic
into them


Author
------

Tool is written by [Labian Gashi](https://gitlab.com/labiangashi).

Feedback is welcome.
