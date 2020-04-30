# Synthetic Data Generation for Databases

Pure Synthetic Data Generation with a PostgreSQL and Python-based Tool

The *Tennis_ATP* dataset can be found inside resources/ and can be set-up very easily using the *import.bat* file (if on Windows) or importing the *.csv* files directly into Postgres (which should be pretty straight-forward).

Usage
------
Tool arguments:
*  **DBNAMEGEN** - Name of the database to be created
*  **-show/--show** - Shows database stats (default)
*  **-generate/--generate** - Generates new synthesized data to database DBNAMEGEN
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
   * Shows statistics from certain tables in database test
* **python pgsynthdata.py db pw1234 -H myHost -p 8070 -U testuser -show**
  * Connects to database *db*, host=*myHost*, port=*8070*, user=*testuser* with password
*pw1234*
  * Shows statistics from certain tables in database *db*
* **python pgsynthdata.py dbin dbgen pw1234 -H myHost -p 8070 -U testuser -generate**
  * Connects to database *dbin*, host=*myHost*, port=*8070*, user=*testuser* with
password *pw1234*
  * Creates new database *dbgen* with synthesized data


Author
------

Plugin is written by [Labian Gashi](https://gitlab.com/labiangashi).

Feedback is welcome.
