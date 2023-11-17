import asyncio
import struct
import sys
import aioodbc
import accex_config
from PySide6.QtWidgets import (
    QApplication, 
    QWidget, 
    QVBoxLayout, 
    QLabel, 
    QPushButton, 
    QLineEdit, 
    QFileDialog, 
    QGraphicsView, 
    QGraphicsScene, 
    QHBoxLayout
)
# import argparse

async def get_table_name_set(cur: aioodbc.Cursor, tableType='TABLE') -> set:
    await cur.tables(tableType=tableType)
    return set([row.table_name for row in await cur.fetchall()])


if __name__ == '__main__':
    
    async def main():

        print('--- reading config')
    
        config = accex_config.parse_config_file('config.accex')

        print('--- config\n', config)

        source_dsn = config.source_dsn

        sources = config.sources

        targets = config.targets

        source_conn_str: str = ''
        conn: aioodbc.Connection = None
        cur: aioodbc.Cursor = None
        async def close_connections():
            if cur: await cur.close()
            if conn: await conn.close()

        table_name_set: set = None

        try:
            for source_table_name in sources.keys():
                print(f'--- getting table [{source_table_name}]')
                source_table = sources[source_table_name]
                source_database = source_table.database # the path to the access database file
                target_table_name = source_table.target # the target table name
                source_columns = source_table.columns
                
                # print(f'--- source database [{source_database}]')

                new_source_conn_str = f'DSN={source_dsn};DBQ={source_database}'

                if new_source_conn_str != source_conn_str:
                    # connect to access database
                    source_conn_str = new_source_conn_str
                    await close_connections()
                    print(f'--- connecting to {source_database} - connection string [{source_conn_str}]')
                    conn = await aioodbc.connect(dsn=source_conn_str)
                    cur = await conn.cursor()

                    # await cur.tables(tableType='TABLE')
                    # print('TABLES:\n  ' + '\n  '.join([row.table_name for row in await cur.fetchall()]))
                
                    table_name_set = await get_table_name_set(cur)
                
                # check if table exists in access
                if source_table_name not in table_name_set:
                    raise ValueError(f'Database [{source_database}] deos not have a table named [{source_table_name}]')
                # check if the table is in the targets
                if target_table_name not in targets:
                    raise ValueError(f'From source [{source_table_name}], there is no target table named [{target_table_name}]')

                # check if columns are valid
                await cur.columns(table=source_table_name)
                true_columns = dict([(row.column_name, row) for row in await cur.fetchall()])
                for column_name in source_columns.keys():
                    # check if column exists in access
                    if column_name not in true_columns:
                        raise ValueError(f'Source [{source_table_name}] deos not have a column named [{column_name}]')
                    # check if column name is in the target table
                    target_column_name = source_columns[column_name]
                    if target_column_name not in targets[target_table_name]:
                        raise ValueError(f'Target [{target_table_name}] deos not have a column named [{target_column_name}]')
        finally:

            # close connections
            print('--- closing connections')
            await close_connections()
    
    asyncio.run(main())
        
    

    






# connection = aioodbc.connect('DSN=MS Access Database;DBQ=./databases/AUTOSHOP.ACCDB')

# print('connected to access database')



# cursor = connection.cursor()

# # cursor.execute('SELECT TOP 3 * FROM Customer')

# cursor.tables(tableType = 'TABLE')


# print([column[0] for column in cursor.description])

# rows = cursor.fetchall()

# print('\n'.join([str(row.table_name) for row in rows]))

# cursor.columns('Customer')

# print([column[0] for column in cursor.description])

# print('\n'.join([' '.join([str(x) for x in [row.column_name, row.data_type, row.type_name, row.sql_data_type, connection.get_output_converter(row.sql_data_type)]]) for row in cursor.fetchall()]))

# def converter(data: bytes):
#     return 'COOL var char: ' + data.decode('utf-16le')

# def converter2(data: bytes):
#     return int.from_bytes(data, byteorder='little')

# connection.add_output_converter(-9, converter)
# connection.add_output_converter(5, converter2)
# print(cursor.execute('SELECT TOP 1 * FROM Customer').fetchone())

# for row in rows:
#     print(row)
    # print(row.cursor_description)
    # print('id', row.SourceID)


# connection.close()

class App():
    def __init__(self):

        self.access_connection = None
        self.access_cursor = None

        self.app = QApplication([])

        # def on_exit():
        #     asyncio.run(self.handle_exit())

        # self.app.aboutToQuit.connect(on_exit)
        window = QWidget()
        self.window = window

        self.main_layout = QVBoxLayout()
        window.setLayout(self.main_layout)


        self.access_connection_input = QLineEdit()
        self.main_layout.addWidget(self.access_connection_input)

        self.access_connection_enter_button = QPushButton('Connect')
        self.main_layout.addWidget(self.access_connection_enter_button)

        def on_access_connection_enter_button_click():
            asyncio.run(self.connect_to_access(self.access_connection_input.text()))

        self.access_connection_enter_button.clicked.connect(on_access_connection_enter_button_click)

        
        # load config
        config = None

        load_config_button = QPushButton('Load Config')
        self.main_layout.addWidget(load_config_button)

        def on_load_config_button_click():
            file_dialog = QFileDialog()
            file_name = QFileDialog.getOpenFileName(file_dialog, 'Open Config', '.', 'Text Files (*.accex);;All Files (*.*)')
            file_path = file_name[0]
            config = accex_config.parse_config_file(file_path)
            print(config)

        load_config_button.clicked.connect(on_load_config_button_click)

        



        self.main_layout.addWidget(QLabel('Tables'))

        self.table_layout = QVBoxLayout()
        self.main_layout.addLayout(self.table_layout)

        window.show()
    
    async def connect_to_access(self, connection_string):
        # 'DSN=MS Access Database;DBQ=./databases/AUTOSHOP.ACCDB'
        connection = await aioodbc.connect(dsn=connection_string)
        print('connected to access')
        cursor = await connection.cursor()

        self.access_connection = connection
        self.access_cursor = cursor

        await cursor.tables(tableType = 'TABLE')
        rows = await cursor.fetchall()

        for row in rows:
            label = QLabel(row.table_name)
            self.table_layout.addWidget(label)

        
    async def disconnect_from_access(self):
        if self.access_connection:
            if self.access_cursor:
                await self.access_cursor.close()
                self.access_cursor = None
            await self.access_connection.close()
            self.access_connection = None

    async def handle_exit(self):
        await self.disconnect_from_access()

# app = App()

# app.app.exec()