.. _config:

Config Files
============

Config files are made up of blocks and records, or dictionaries and properties. The syntax is similar to YAML or JSON. The recommended naming convention for config files is ``*.accex``, for example ``config.accex``. The program will search for config files in the current working directory by the file extension if a path is not specified.

The purpose of config files are to:

- Specify how the target database should be created, such as the tables and columns
- Specify which source tables and columns should map to the target tables and columns
- Specify ODBC connection string parameters

Basic Syntax
------------

.. code-block::
   :dedent: 1
   :caption: config.accex

    # comments defined with hash symbol
    
    # records match a value to a key with colon
    
    Example_Record: my value # comments can go at the end as well
    
    # hash symbols can be escaped with a backslash

    example_record2: another value \# with escaped hash symbol

    # blocks don't have a colon and are surrounded by braces
    # blocks can contain records or more blocks

    Example_Block {
        recordInside: value
        BlockInside {
            record_inside_inside: inside value!
        }
    }

Sources Example
---------------

For source tables, each ``COLUMNS`` record has key that represents the column name in the source table, and a value that represents the target column name in the target table. The target column name extends from the defined ``TARGET_TABLE`` record, in this case ``automobiles``.
The identifiers in uppercase letters are keywords that the program reads.

.. code-block::
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 8

    SOURCE_DSN_PARAMS {
        Driver: Microsoft Access Driver (*.mdb, *.accdb)
        DBQ: ./databases/autoshop.accdb
    }

    SOURCES {
        Automobile {
            TARGET_TABLE: automobiles
            COLUMNS {
                AutoID: old_id
                AutoVIN: vin
                AutoMake: make
                AutoModel: model
                AutoYear: year
                AutoLicPlate: plate
                State: state
                AutoMileage: mileage
                Notes: notes
            }
        }
    }

Targets Example
---------------

For target tables, each ``COLUMNS`` record has a key representing the target table column name, and a value that represents the SQL type for the column when the table is created.

.. code-block::
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 11

    TARGET_DSN_PARAMS {
        Driver: PostgreSQL Unicode
        Server: localhost
        Port: 5432
        Database: postgres
        Uid: postgres
        Pwd: root
    } 

    TARGETS {
        automobiles {
            COLUMNS {
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
            }
        }
    }

Note that this is all in one config file. When it's read, the program:

1. Reads the DSN parameters for the source and target databases and constructs a connection string, then connects to the databases
2. Creates target table, in this case ``automobiles``, with the given target columns
3. Selects rows from the source database table ``Automobile``, with the given source columns
4. Inserts each row selected to the target table, casting any values to the new type. For example, ``AutoVIN`` is transferred to ``vin``
5. Repeats for all specified tables

====

Multiple Sources
----------------

If you have multiple source databases, they can each be defined in one config file by defining DSN parameters for each source table using a ``DSN_PARAMS`` block. The parameters in ``DSN_PARAMS`` will override any parameter with the same key from ``SOURCE_DSN_PARAMS``.

.. code-block::
   :dedent: 1
   :caption: config.accex
   :emphasize-lines: 7-9

    SOURCE_DSN_PARAMS {
        Driver: Microsoft Access Driver (*.mdb, *.accdb)
    }

    SOURCES {
        Automobile {
            DSN_PARAMS {
                DBQ: ./databases/autoshop.accdb
            }
            TARGET_TABLE: automobiles
            COLUMNS {
                ...
            }
        }
        Employees {
            DSN_PARAMS {
                DBQ: ./databases/shopdata.accdb
            }
            TARGET_TABLE: employees
            COLUMNS {
                ...
            }
        }
    }

Environment Variables
---------------------

Environment variables can be defined with a dollar sign ``$VARIABLE_NAME`` or with a dollar sign and braces ``${VARIABLE_NAME}``. The program will replace them when parsed. Using a ``.env`` file is also supported.

.. code-block:: sh
   :dedent: 1
   :caption: .env

    POSTGRES_PORT=8000
    POSTGRES_PASSWORD=123

.. code-block::
   :dedent: 1
   :caption: config.accex

    TARGET_DSN_PARAMS {
        Driver: PostgreSQL Unicode
        Server: localhost
        Port: $POSTGRES_PORT
        Database: postgres
        Uid: postgres
        Pwd: ${POSTGRES_PASSWORD}
    }