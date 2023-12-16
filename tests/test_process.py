import logging
import pytest
import accex.config.core as ac
import accex.process.core as ap

@pytest.fixture(scope="session")
def shared():
    return {}

def test_get_connection_string():
    config = ac.Config({
        "SOURCE_DSN_PARAMS": {
            "a": "1",
            "b": "2"
        },
        "SOURCE_DATABASES": {
            "db_a": {
                "DSN_PARAMS": {
                    "b": "3"
                }
            }
        },
        "SOURCES": [
            {
                "TABLE": "db_a.table_a",
                "DSN_PARAMS": {
                    "c": "4"
                }
            }
        ]
    })
    assert ap.get_src_conn_str(config) == "a=1;b=2"
    assert ap.get_src_conn_str(config, "db_a.table_a") == "a=1;b=3;c=4"
    assert not ap.get_src_conn_str(config, "table_a")
    config.sources[0].table_pointer.catalog_name = ""
    assert ap.get_src_conn_str(config, "table_a") == "a=1;b=2;c=4"

    config = ac.Config({
        "TARGET_DSN_PARAMS": {
            "a": "1",
            "b": "2"
        },
        "TARGETS": [
            {
                "TABLE": "table_a",
                "DSN_PARAMS": {
                    "c": "4"
                }
            },
            {
                "TABLE": "table_b",
                "DSN_PARAMS": {
                    "b": "5",
                    "c": "6"
                }
            },
            {
                "TABLE": "catalog_a.schema_a.table_a",
                "DSN_PARAMS": {
                    "b": "8",
                    "c": "9"
                }
            },
            {
                "TABLE": "schema_a.table_a",
                "DSN_PARAMS": {
                    "b": "7",
                    "c": "8"
                }
            }
        ]
    })
    assert ap.get_tgt_conn_str(config) == "a=1;b=2"
    assert ap.get_tgt_conn_str(config, "table_a") == "a=1;b=2;c=4"
    assert ap.get_tgt_conn_str(config, "table_b") == "a=1;b=5;c=6"
    assert ap.get_tgt_conn_str(config, "schema_a.table_a") == "a=1;b=7;c=8"
    assert ap.get_tgt_conn_str(config, "catalog_a.schema_a.table_a") == "a=1;b=8;c=9"

@pytest.mark.asyncio
async def test_transfer(shared):
    config_path = "./tests/configs/config.accex"
    with open(config_path, "r") as config_file:
        config = ac.parse_config(config_file.read())
    
    shared["config"] = config

    await ap.transfer(config)
    # verify that data matches
    for src in config.sources:
        src_name = src.table_pointer.table_name
        src_conn_str = ap.get_src_conn_str(config, src.table_pointer)
        tgt_conn_str = ap.get_tgt_conn_str(config, src.target_pointer)

        src_cur = await ap.open_src_connection(src_conn_str)
        tgt_cur = await ap.open_tgt_connection(tgt_conn_str)

        logging.info("checking target table created")
        # check target table created 
        tgt_tables = await ap.get_table_name_dict(tgt_cur)
        # FIXME: support schema in the future
        assert src.target_pointer.table_name in tgt_tables
        
        logging.info("checking target columns")
        # check columns created
        tgt_cols = await ap.get_column_name_dict(tgt_cur, src.target_pointer.table_name, src.target_pointer.schema_name)
        for col in src.columns.values():
            if isinstance(col, ac.TargetColumnPointer):
                assert col.column_name in tgt_cols, f"missing \"{col.column_name}\" in {src.target_pointer}"
            elif isinstance(col, ac.SourceColumnMapFunction):
                assert col.to_column.column_name in tgt_cols, f"missing \"{col.to_column.column_name}\" in {src.target_pointer}"
        
        logging.info("checking table counts")
        # check row count
        await src_cur.execute(f"SELECT COUNT(*) AS count FROM {src_name}")
        src_count = (await src_cur.fetchone())[0]
        await tgt_cur.execute(f"SELECT COUNT(*) AS count FROM {src.target_pointer.to_sql_str()}")
        tgt_count = (await tgt_cur.fetchone())[0]
        assert src_count == tgt_count, f"{src} count ({src_count}) does not match {src.target_pointer} count ({tgt_count})"

    shared[test_transfer.__name__] = True

    await ap.close_connections()

@pytest.mark.asyncio
async def test_transfer_foreign_keys(shared):
    assert shared.get("config")
    assert shared.get(test_transfer.__name__)

    config = shared.get("config")

    src_conn_str = ap.get_src_conn_str(config, "Automobile")
    tgt_conn_str = ap.get_tgt_conn_str(config)

    src_cur = await ap.open_src_connection(src_conn_str)
    tgt_cur = await ap.open_tgt_connection(tgt_conn_str)

    await src_cur.execute(f"SELECT Automobile.AutoID AS autoid, Customer.CustID AS custid FROM Automobile INNER JOIN Customer ON Automobile.CustID = Customer.CustID")
    src_autoid_to_custid = dict(await src_cur.fetchall())

    await tgt_cur.execute(f"SELECT automobiles.old_id AS autoid, customers.old_id AS custid FROM automobiles JOIN customers ON automobiles.customer_id = customers.id")
    tgt_autoid_to_custid = dict(await tgt_cur.fetchall())

    assert src_autoid_to_custid == tgt_autoid_to_custid

    await ap.close_connections()
    
