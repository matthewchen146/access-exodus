import asyncio
import struct
import sys
import typing
import logging
from functools import partial

import typeguard
from typeguard import check_type
import aioodbc
import pyodbc
import accex_config
from PySide6.QtWidgets import (
    QApplication,
    QToolBar,
    QWidget,
    QLayout,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
    QMainWindow,
    QFileDialog,
    QMenuBar,
    QMenu,
    # QGraphicsView,
    # QGraphicsScene,
    QHBoxLayout,
    QTreeWidget,
    QTreeWidgetItem,
    QTableWidget,
    QTableWidgetItem,
    QGridLayout
)
from PySide6.QtGui import (
    QAction, QIcon, QPixmap
)

# from PySide6.QtQml import QQmlApplicationEngine
from PySide6 import QtCore
from PySide6.QtCore import Qt

QtCore.QCoreApplication.setAttribute(QtCore.Qt.ApplicationAttribute.AA_DontUseNativeMenuBar, True)

# import argparse
from edifice import (
    component,
    ExportList,
    Window,
    App,
    Label,
    TextInput,
    View,
    CustomWidget,
    Button,
    TableGridView,
    use_state,
    use_effect,
    use_ref
)

TExpectedType = typing.TypeVar("TExpectedType")
def check_type_bool(value, expected_type: TExpectedType) -> typing.TypeGuard[TExpectedType]:
    try:
        check_type(value, expected_type)
        return True
    except typeguard.TypeCheckError:
        return False

def delete_widget(widget: QWidget):
    # check if the widget has a parent
    if widget.parent():
        # remove the widget from its parent layout or widget
        widget.setParent(None)
    # delete the widget
    widget.deleteLater()

def delete_layout(layout: QLayout):
    while layout.count():
        child = layout.takeAt(0)
        if child.widget():
            delete_widget(child.widget())
        elif child.layout():
            delete_layout(child.layout())
    del layout

def delete_object(object: QtCore.QObject):
    if isinstance(object, QWidget):
        delete_widget(object)
    elif isinstance(object, QLayout):
        delete_layout(object)
    else:
        object.deleteLater()

@component
def EdificeWidgets(self):
    config_init: typing.Optional[accex_config.Config] = None
    config, set_config = use_state(config_init)

    def on_load_config_button_click(e):
        file_dialog = QFileDialog()
        file_name = QFileDialog.getOpenFileName(file_dialog, 'Open Config', '.',
                                                'Text Files (*.accex);;All Files (*.*)')
        file_path = file_name[0]
        c = accex_config.parse_config_file(file_path)
        print(c)
        set_config(c)

    def on_new_config_button_click(e):
        set_config(accex_config.Config())

    with ExportList():
        with View(layout="column", style={}):
            Button('Load Config', on_click=on_load_config_button_click)
            Button('New Config', on_click=on_new_config_button_click)
        Label("Label")

TIcon = QIcon | QPixmap

TMenuAction = typing.Callable


class TActionOptions(typing.TypedDict):
    func: TMenuAction
    tip: typing.Optional[str]
    checkable: typing.Optional[bool]


TMenuOptions = dict[
    str | typing.Tuple[TIcon, str],
    dict[str, typing.Union[TMenuAction, TActionOptions]]
]


def add_menu_from_dict(window: QMainWindow, menu_options: TMenuOptions):
    menu_bar = window.menuBar()
    for label, data in menu_options.items():

        if check_type_bool(label, str):
            menu = menu_bar.addMenu(label)
        elif check_type_bool(label, typing.Tuple[TIcon, str]):
            # menu icon and title
            menu = menu_bar.addMenu(label[0], label[1])
        else:
            raise ValueError("Invalid menu options")

        for name, action_data in data.items():
            action = QAction(name, window)
            if check_type_bool(action_data, TMenuAction):
                action.triggered.connect(action_data)
            elif check_type_bool(action_data, TActionOptions):
                action.triggered.connect(action_data["func"])
                if "tip" in action_data:
                    action.setStatusTip(action_data["tip"])
                if "checkable" in action_data:
                    action.setCheckable(action_data["checkable"])
            else:
                raise ValueError(f"Invalid menu action option [{name}] - data: [{str(action_data)}]")
            menu.addAction(action)


def create_config_view(config: accex_config.Config) -> QWidget:
    w = QWidget()
    w.setLayout(QVBoxLayout())

    tables_widget = QWidget()

    tables_layout = QVBoxLayout()

    tables_widget.setLayout(tables_layout)

    title_style_sheet = "border-bottom-width: 1px; border-bottom-style: solid; border-radius: 0px; font-size: 16px"

    col_0_width = 140
    col_1_width = 140

    def create_row(src_col, tgt_col) -> QHBoxLayout:

        src_col_label = QLabel(src_col)
        src_col_label.setMinimumWidth(col_0_width)
        src_col_label.setMaximumWidth(col_0_width)
        tgt_col_label = QLabel(tgt_col)
        tgt_col_label.setMinimumWidth(col_1_width)
        tgt_col_label.setMaximumWidth(col_1_width)

        delete_button = QPushButton("delete")
        delete_button.clicked.connect(lambda: delete_layout(row))

        # contained in horizontal layout
        row = QHBoxLayout()
        row.addWidget(src_col_label)
        row.addWidget(tgt_col_label)
        row.addWidget(delete_button)

        return row

    for src_table_name, src_table in config.sources.items():
        title_label = QLabel(src_table_name)
        title_label.setStyleSheet(title_style_sheet)

        tables_layout.addWidget(title_label)
        
        # layout for column records
        table_records_layout = QVBoxLayout()
        table_records_layout.setSpacing(0)
        tables_layout.addLayout(table_records_layout)
        
        for src_col, tgt_col in src_table.columns.items():
            # create each record
            row = create_row(src_col, tgt_col)

            table_records_layout.addLayout(row)

    w.layout().addWidget(tables_widget)

    return w


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logging.info("creating app")
    app = QApplication([])

    # setup central widget
    central_widget = QWidget()
    central_widget.setLayout(QVBoxLayout())

    # test add exported edifice widgets
    # exported_widgets = App(EdificeWidgets(), create_application=False).export_widgets()
    # for widget in exported_widgets:
    #     central_widget.layout().addWidget(widget)

    current_config: accex_config.Config | None = None
    config_widget: QWidget | None = None
    def set_config(config: accex_config.Config):
        global config_widget
        current_config = config
        if config_widget:
            delete_widget(config_widget)
        config_widget = create_config_view(current_config)
        central_widget.layout().addWidget(config_widget)

    def on_load_config():
        file_dialog = QFileDialog()
        file_name = QFileDialog.getOpenFileName(file_dialog, 'Open Config', '.',
                                                'Text Files (*.accex);;All Files (*.*)')
        file_path = file_name[0]
        try:
            config = accex_config.parse_config_file(file_path)
            set_config(config)
        except ValueError:
            logging.error("failed to load config")

    # setup window
    window = QMainWindow()
    menu: TMenuOptions = {
        "File": {
            "Open": on_load_config,
            "Save": lambda: logging.info("saving"),
            "New": lambda: logging.info("new")
        }
    }
    add_menu_from_dict(window, menu)
    window.setCentralWidget(central_widget)
    window.show()

    logging.info("starting app")
    app.exec()
