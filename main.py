import asyncio
import struct
import sys
import typing

import typeguard
from typeguard import check_type
import aioodbc
import pyodbc
import accex_config
from PySide6.QtWidgets import (
    QApplication,
    QToolBar,
    QWidget,
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
)
from PySide6.QtGui import (
    QAction, QIcon, QPixmap
)

# from PySide6.QtQml import QQmlApplicationEngine
from PySide6 import QtCore

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


@component
def AccexApp(self):
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


def add_menu(window: QMainWindow, title: str, icon: QIcon | QPixmap = None) -> QMenu:
    if icon:
        return window.menuBar().addMenu(icon, title)
    else:
        return window.menuBar().addMenu(title)


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


if __name__ == "__main__":
    print("starting app")
    app = QApplication([])
    window = QMainWindow()
    exported_widgets = App(AccexApp(), create_application=False).export_widgets()

    central_widget = QWidget()
    window.setCentralWidget(central_widget)
    central_widget.setLayout(QVBoxLayout())
    for widget in exported_widgets:
        central_widget.layout().addWidget(widget)

    button_action = QAction("&Your button", window)
    button_action.setStatusTip("This is your button")
    button_action.triggered.connect(lambda: print("pressed"))
    button_action.setCheckable(True)

    # file_menu = add_menu(w, "File")
    # file_menu.addAction(button_action)

    menu: TMenuOptions = {
        "File": {
            "Save": lambda e: print("saving", e),
            "New": lambda: print("new")
        }
    }
    add_menu_from_dict(window, menu)

    window.show()

    app.exec()
