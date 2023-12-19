.. _config:

Config Files
============

Config files follow YAML syntax. The recommended naming convention for config files is ``*.accex``, for example ``config.accex``. 
The program will search for config files in the current working directory by the file extension if a path is not specified.

The purpose of config files are to:

- Specify how the target database should be created, such as the tables and columns
- Specify which source tables and columns should map to the target tables and columns
- Specify ODBC connection string parameters

Basic Structure
---------------

.. code-block:: yaml
   :dedent: 1
   :caption: config.accex

    # root params for the source database ODBC connection string
    SOURCE_DSN_PARAMS:
      param: my param
    
    # predefined source databases to hold reusable settings and params
    SOURCE_DATABASES:
      db_id:
        DSN_PARAMS:
          ...

    # list of tables that represent source tables and columns
    SOURCES:
    - TABLE: db_id.table
      DSN_PARAMS:
        ...
      TARGET_TABLE: my_target_table
      COLUMNS:
        # column values point to a column defined in a table defined in the TARGETS section
        column_name: target.column.pointer
        ...
    - TABLE2: table2
      ...

    # root params for the target database ODBC connection string
    TARGET_DSN_PARAMS:
      ...
    
    # set of tables that represent target tables and columns
    # defined as a list of tables, but no duplicate tables are allowed
    TARGETS:
    - TABLE: my_target_table
      DSN_PARAMS:
        ...
      COLUMNS:
        # each column represents a column created in the target table
        # the value is the type and constraints that would be defined inline with the column
        # they are used verbatim as the target database's language
        column_name: SERIAL PRIMARY KEY
        ...
    - TABLE: table2
      ...

Sources Example
---------------

For source tables, each ``COLUMNS`` record has key that represents the column name in the source table, and a value that represents the target column name in the target table. 
The target column name extends from the defined ``TARGET_TABLE`` record, in this case ``automobiles``.
The identifiers in uppercase letters are keywords that should be followed.

.. code-block:: yaml
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 8

    SOURCE_DSN_PARAMS:
      Driver: Microsoft Access Driver (*.mdb, *.accdb)
      DBQ: ./databases/autoshop.accdb

    SOURCES:
    - TABLE: Automobile
      TARGET_TABLE: automobiles
      COLUMNS:
        AutoID: old_id
        AutoVIN: vin
        AutoMake: make
        AutoModel: model
        AutoYear: year
        AutoLicPlate: plate
        State: state
        AutoMileage: mileage
        Notes: notes

Targets Example
---------------

For target tables, each ``COLUMNS`` record has a key representing the target table column name, and a value that represents the SQL type for the column when the table is created.

.. code-block:: yaml
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 11

    TARGET_DSN_PARAMS:
      Driver: PostgreSQL Unicode
      Server: localhost
      Port: 5432
      Database: postgres
      Uid: postgres
      Pwd: root

    TARGETS:
    - TABLE: automobiles
      COLUMNS:
        id: serial primary key
        old_id: text
        vin: text
        make: text
        model: text
        year: text
        plate: text
        state: text
        mileage: int
        notes: text

Note that this is all in one config file. When it's read, the program:

1. Reads the DSN parameters for the source and target databases and constructs a connection string, then connects to the databases
2. Creates target table, in this case ``automobiles``, with the given target columns
3. Selects rows from the source database table ``Automobile``, with the given source columns
4. Inserts each row selected to the target table, casting any values to the new type. For example, ``AutoVIN`` is transferred to ``vin``
5. Repeats for all specified tables

====

Multiple Sources
----------------

If you have multiple source databases, they can each be defined in one config file by defining DSN parameters for each source table using a ``DSN_PARAMS`` block. 
The parameters in ``DSN_PARAMS`` will override any parameter with the same key from ``SOURCE_DSN_PARAMS``.

Source databases can also be defined under ``SOURCE_DATABASES`` so that their params can be reused easily, for example in ``database_a``.
The database can be referenced with dot notation in the source table's definition, such as ``database_a.Automobile``
Params defined in the ``SOURCES`` table itself will take precedence over the ``SOURCE_DATABASES`` params, and those will take precedence over ``SOURCE_DSN_PARAMS``.

.. code-block:: yaml
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 5,10

    SOURCE_DSN_PARAMS:
      Driver: Microsoft Access Driver (*.mdb, *.accdb)

    SOURCE_DATABASES:
      database_a:
        DSN_PARAMS:
          DBQ: ./databases/autoshop.accdb

    SOURCES:
    - TABLE: database_a.Automobile
      DSN_PARAMS:
        ExtraParam: extra param
      TARGET_TABLE: automobiles
      COLUMNS:
        ...
    - TABLE: Employees
      DSN_PARAMS:
        DBQ: ./databases/shopdata.accdb
      TARGET_TABLE: employees
      COLUMNS:
        ...

Environment Variables
---------------------

Environment variables can be defined with a dollar sign ``$VARIABLE_NAME`` or with a dollar sign and braces ``${VARIABLE_NAME}``. The program will replace them when parsed. Using a ``.env`` file is also supported.

.. code-block:: sh
   :dedent: 1
   :caption: .env

    POSTGRES_PORT=8000
    POSTGRES_PASSWORD=123

.. code-block:: yaml
   :dedent: 1
   :caption: config.accex

    TARGET_DSN_PARAMS:
      Driver: PostgreSQL Unicode
      Server: localhost
      Port: $POSTGRES_PORT # 8000
      Database: postgres
      Uid: postgres
      Pwd: ${POSTGRES_PASSWORD} # 123