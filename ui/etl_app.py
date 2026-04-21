from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel,
    QListWidget, QPushButton, QHBoxLayout,
    QLineEdit, QMessageBox, QComboBox, QSizePolicy, QRadioButton
)

from utils.config_manager import load_config, save_config, apply_aimi_state
import sys

import builders.asset_register as asset_register
import builders.inspection_data as inspection_data
import builders.asset_task as asset_task
import builders.pof_assessment as pof_assessment
import builders.cof_assessment as cof_assessment

from builders.asset_register import build_asset_register
from builders.inspection_data import build_inspection_data
from builders.asset_task import build_asset_task
from builders.pof_assessment import build_pof_assessment
from builders.cof_assessment import build_cof_assessment

from utils.db import create_sql_engine as get_engine, validate_sql_engine, sql_server_busy, get_sql_columns
from pathlib import Path
from datetime import datetime
import pandas as pd

WORKBOOKS = {
    "asset_register": {
        "builder": build_asset_register,
        "filename": "Asset Register",
    },
    "inspection_data": {
        "builder": build_inspection_data,
        "filename": "Inspection Data",
    },
    "asset_task": {
        "builder": build_asset_task,
        "filename": "Asset Task",
    },
    "pof_assessment": {
        "builder": build_pof_assessment,
        "filename": "PoF Assessment",
    },
    "cof_assessment": {
        "builder": build_cof_assessment,
        "filename": "CoF Assessment",
    }
}

BUILDERS = [
    asset_register,
    inspection_data,
    asset_task,
    pof_assessment,
    cof_assessment
]

class QtWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL (PCMS → Newton)")

        self.config = load_config()
        self.engine = get_engine(self.config)
        
        layout = QVBoxLayout()

        # [ Section Dropdown ]
        layout.addWidget(QLabel("Configuration Section"))

        self.section_selector = QComboBox()
        self.section_selector.addItems([
            "Unit Filter",
            "Mappings",
            "Default Values",
            "Newton Columns",
            "Schema Map"
        ])

        self.section_selector.currentTextChanged.connect(self.load_selected_section)

        layout.addWidget(self.section_selector)

        # [ Dynamic Config Editor Area ]
        self.dynamic_container = QVBoxLayout()
        layout.addLayout(self.dynamic_container)

        # ---- Connection ----
        layout.addWidget(QLabel("Server"))
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("Enter Server")
        self.server_input.setText(self.config["connection"]["server"])
        layout.addWidget(self.server_input)
        
        layout.addWidget(QLabel("Database"))
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("Enter Database")
        self.database_input.setText(self.config["connection"]["database"])
        layout.addWidget(self.database_input)
        # ---- End Connection ----

        # ---- Test Button ----
        test_btn = QPushButton("Test SQL Connection")
        test_btn.clicked.connect(self.test)
        layout.addWidget(test_btn)
        # ---- End Test Button ----
        
        # ---- Save Button ----
        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self.save)
        layout.addWidget(save_btn)
        # ---- End Save Button ----

        # ---- Run Workbook Section ----
        radio_row = QHBoxLayout()
        radio_row.addWidget(QLabel("Run Workbook"))
        self.pre_radio = QRadioButton("Pre-AIMI")
        self.post_radio = QRadioButton("Post-AIMI")
        radio_row.addWidget(self.pre_radio)
        radio_row.addWidget(self.post_radio)
        layout.addLayout(radio_row)
        self.post_radio.toggled.connect(self.handle_radio) # Signal for toggled state
        
        run_row = QHBoxLayout()

        asset_btn = QPushButton("Asset Register")
        asset_btn.clicked.connect(self.run_asset_register)

        inspection_btn = QPushButton("Inspection Data")
        inspection_btn.clicked.connect(self.run_inspection_data)
        
        task_btn = QPushButton("Asset Task")
        task_btn.clicked.connect(self.run_asset_task)
        
        pof_btn = QPushButton("PoF Assessment")
        pof_btn.clicked.connect(self.run_pof_assessment)
        
        cof_btn = QPushButton("CoF Assessment")
        cof_btn.clicked.connect(self.run_cof_assessment)

        all_btn = QPushButton("All Workbooks")
        all_btn.clicked.connect(self.run_all)

        run_row.addWidget(asset_btn)
        run_row.addWidget(inspection_btn)
        run_row.addWidget(task_btn)
        run_row.addWidget(pof_btn)
        run_row.addWidget(cof_btn)
        run_row.addWidget(all_btn)

        layout.addLayout(run_row)
        # ---- End Run Workbook Section ----

        self.setLayout(layout)
        self.load_selected_section(self.section_selector.currentText())
        self.pre_radio.setChecked(True)
        self.handle_radio()

    def clear_layout(self, layout):
        while layout.count():
            item = layout.takeAt(0)

            widget = item.widget()
            child_layout = item.layout()

            if widget is not None:
                widget.deleteLater()

            elif child_layout is not None:
                self.clear_layout(child_layout)
            
    def load_selected_section(self, section_name):
        # Clear old widgets
        self.clear_layout(self.dynamic_container)

        if section_name == "Unit Filter":
            self.build_filters_ui()
        elif section_name == "Mappings":
            self.build_mappings_ui()
        elif section_name == "Default Values":
            self.build_default_values_ui()
        elif section_name == "Newton Columns":
            self.build_newton_columns_ui()
        elif section_name == "Schema Map":
            self.build_schema_map_ui()
            
    # ---- Unit Filters ----
    def build_filters_ui(self):
        self.dynamic_container.addWidget(QLabel("Units Filter"))

        self.unit_list = QListWidget()
        for unit in self.config.get("filters", {}).get("units", []):
            self.unit_list.addItem(unit)

        self.dynamic_container.addWidget(self.unit_list)

        self.unit_input = QLineEdit()
        self.unit_input.setPlaceholderText("Enter Unit ID")
        self.dynamic_container.addWidget(self.unit_input)

        btn_row = QHBoxLayout()

        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self.add_unit)

        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_unit)

        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)

        self.dynamic_container.addLayout(btn_row)
        
    def add_unit(self):
        value = self.unit_input.text().strip()
        if not value:
            # QMessageBox.warning(self, "Invalid", "Unit cannot be empty")
            return

        self.unit_list.addItem(value)
        self.unit_input.clear()
        
        # Select added unit
        last_index = self.unit_list.count() - 1
        self.unit_list.setCurrentRow(last_index)
        self.save(True)

    def remove_unit(self):
        selected = self.unit_list.selectedItems()
        
        if not selected:
            return
        
        for item in selected:
            self.unit_list.takeItem(self.unit_list.row(item))
            
        self.save(True) 
    # ---- End Unit Filters ----

    # ---- Mappings ----
    def build_mappings_ui(self):
        self.dynamic_container.addWidget(QLabel("Mappings"))

        self.mapping_selector = QComboBox()
        
        for key in self.config.get("mappings", {}).keys():
            display_text = key.replace("_", " ")
            self.mapping_selector.addItem(display_text, key)
            
        self.mapping_selector.currentTextChanged.connect(self.load_mapping_list)
        self.dynamic_container.addWidget(self.mapping_selector)

        self.mapping_list = QListWidget()
        self.dynamic_container.addWidget(self.mapping_list)
        self.mapping_list.itemSelectionChanged.connect(self.load_mapping)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self.add_mapping)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_mapping)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)
        self.dynamic_container.addLayout(btn_row)

        mapping_label_row = QHBoxLayout()
        mapping_label_row.addWidget(QLabel("PCMS Value"))
        mapping_label_row.addWidget(QLabel("Newton Value"))
        self.dynamic_container.addLayout(mapping_label_row)

        mapping_input_row = QHBoxLayout()
        self.pcms_input = QLineEdit()
        self.newton_input = QLineEdit()
        mapping_input_row.addWidget(self.pcms_input)
        mapping_input_row.addWidget(self.newton_input)
        self.dynamic_container.addLayout(mapping_input_row)
        self.pcms_input.textChanged.connect(self.update_mapping_list)
        self.newton_input.textChanged.connect(self.update_mapping_list)
        
        # Load first mapping automatically
        self.load_mapping_list()

    def load_mapping(self):
        selected = self.mapping_list.currentItem()
        
        if not selected:
            item = self.mapping_list.item(0)
        else:
            item = selected
        
        key, value = [x.strip() for x in item.text().split("→")]
        
        self.pcms_input.setText(str(key))
        self.newton_input.setText(str(value))
    
    def load_mapping_list(self):
        mapping = self.mapping_selector.currentData()
        self.mapping_list.clear()
        mapping_dict = self.config.get("mappings", {}).get(mapping, {})

        for key, value in mapping_dict.items():
            self.mapping_list.addItem(f"{key}  →  {value}")
        
        self.mapping_list.clearSelection()
        self.pcms_input.clear()
        self.newton_input.clear()
        
    def update_mapping_list(self):
        selected = self.mapping_list.currentItem()
        key = self.pcms_input.text().strip()
        value = self.newton_input.text().strip()
        
        if selected:
            selected.setText(f"{key}  →  {value}")
        
    def add_mapping(self):
        key = "<PCMS Value>" # self.pcms_input.text().strip()
        value = "<Newton Value>" # self.newton_input.text().strip()

        self.mapping_list.addItem(f"{key}  →  {value}")
        
        # Select added mapping
        last_index = self.mapping_list.count() - 1
        self.mapping_list.setCurrentRow(last_index)

    def remove_mapping(self):
        for item in self.mapping_list.selectedItems():
            self.mapping_list.takeItem(self.mapping_list.row(item))  
    # ---- End Mappings ----
    
    # ---- Default Values ----
    def build_default_values_ui(self):
        self.dynamic_container.addWidget(QLabel("Default Values"))

        self.default_value_selector = QComboBox()

        for key in self.config.get("default_values", {}).keys():
            display_text = key.replace("_", " ")
            self.default_value_selector.addItem(display_text, key)
            
        self.default_value_selector.currentTextChanged.connect(self.load_default_value_list)
        self.dynamic_container.addWidget(self.default_value_selector)

        self.default_value_list = QListWidget()
        self.dynamic_container.addWidget(self.default_value_list)
        self.default_value_list.itemSelectionChanged.connect(self.load_default_value)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self.add_default_value)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_default_value)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)
        self.dynamic_container.addLayout(btn_row)

        default_value_label_row = QHBoxLayout()
        default_value_label_row.addWidget(QLabel("Newton Column"))
        default_value_label_row.addWidget(QLabel("Default Value"))
        self.dynamic_container.addLayout(default_value_label_row)

        default_value_input_row = QHBoxLayout()
        self.column_selector = QComboBox()
        self.column_selector.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.column_selector.currentTextChanged.connect(self.load_default_value)

        self.default_input = QLineEdit()
        default_value_input_row.addWidget(self.column_selector)
        default_value_input_row.addWidget(self.default_input)
        self.dynamic_container.addLayout(default_value_input_row)
        self.default_input.textChanged.connect(self.update_default_value_list)

        # Load first default_value automatically
        self.load_default_value_list()

    def load_default_value(self):
        value = None
        source = self.sender()
        
        if source == self.default_value_list:
            selected = self.default_value_list.currentItem()
            
            if not selected:
                return
            
            key, value = [x.strip() for x in selected.text().split("→")]
        
            index = self.column_selector.findText(str(key))
        
            if index >= 0:
                self.column_selector.setCurrentIndex(index)
        elif source == self.column_selector:
            self.default_value_list.setCurrentItem(None)
            key = self.column_selector.currentText()
            
            for i in range(self.default_value_list.count()):
                item_text = self.default_value_list.item(i).text()
                parts = item_text.split("→")
                
                if len(parts) != 2:
                    return
            
                if key == parts[0].strip():
                    self.default_value_list.setCurrentRow(i)
                    value = parts[1].strip()
                    break  
        
        if not value:
            self.default_input.clear()
        else:
            self.default_input.setText(str(value))
    
    def load_default_value_list(self):
        defaults = self.default_value_selector.currentData()
        self.default_value_list.clear()
        self.column_selector.clear()
        default_value_dict = self.config.get("default_values", {}).get(defaults, {})
        
        for key, value in default_value_dict.items():
            self.default_value_list.addItem(f"{key}  →  {value}")
            
        for col in self.config.get("newton_columns", {}).get(defaults, []):
            self.column_selector.addItem(col)
            
        self.default_value_list.clearSelection()
        self.column_selector.setCurrentIndex(-1)
        self.default_input.clear()

    def update_default_value_list(self):
        selected = self.default_value_list.currentItem()
        
        if not selected:
            return
        
        key = self.column_selector.currentText()
        value = self.default_input.text().strip()
        selected.setText(f"{key}  →  {value}")
        
    def add_default_value(self):
        key = self.column_selector.currentText().strip()
        value = self.default_input.text().strip()

        if not key:
            QMessageBox.warning(self, "Invalid", "Newton Column cannot be empty")
            return
        
        if not value:
            value = "<Default Value>"
            
        self.default_value_list.addItem(f"{key}  →  {value}")
        
        # Select added default_value
        last_index = self.default_value_list.count() - 1
        self.default_value_list.setCurrentRow(last_index)

    def remove_default_value(self):
        for item in self.default_value_list.selectedItems():
            self.default_value_list.takeItem(self.default_value_list.row(item))  
    # ---- End Default Values ----

    # ---- Newton Columns ----
    def build_newton_columns_ui(self):
        self.dynamic_container.addWidget(QLabel("Newton Column"))

        self.newton_columns_selector = QComboBox()
        
        for key in self.config.get("newton_columns", {}).keys():
            display_text = key.replace("_", " ")
            self.newton_columns_selector.addItem(display_text, key)
            
        self.newton_columns_selector.currentTextChanged.connect(self.load_newton_columns_list)
        self.dynamic_container.addWidget(self.newton_columns_selector)

        self.newton_columns_list = QListWidget()
        self.dynamic_container.addWidget(self.newton_columns_list)
        self.newton_columns_list.itemSelectionChanged.connect(self.load_newton_column)
        
        order_row = QHBoxLayout()
        order_row.addWidget(QLabel("            Change Column Order:"))
        move_up_btn = QPushButton("↑")
        move_up_btn.clicked.connect(self.move_newton_column_up)
        move_down_btn = QPushButton("↓")
        move_down_btn.clicked.connect(self.move_newton_column_down)
        order_row.addWidget(move_up_btn)
        order_row.addWidget(move_down_btn)
        self.dynamic_container.addLayout(order_row)
        
        self.newton_column_input = QLineEdit()
        self.dynamic_container.addWidget(self.newton_column_input)
        self.newton_column_input.textChanged.connect(self.update_newton_columns_list)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self.add_newton_column)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_newton_column)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)
        self.dynamic_container.addLayout(btn_row)

        # Load first newton_columns automatically
        self.load_newton_columns_list()
        
    def load_newton_column(self):
        selected = self.newton_columns_list.currentItem()
        
        if not selected:
            return

        self.newton_column_input.setText(str(selected.text()))
        
    def load_newton_columns_list(self):
        newton_columns = self.newton_columns_selector.currentData()
        self.newton_columns_list.clear()
        newton_columns_dict = self.config.get("newton_columns", {}).get(newton_columns, [])
        self.newton_columns_list.addItems(list(newton_columns_dict))

    def update_newton_columns_list(self):
        selected = self.newton_columns_list.currentItem()
        
        if selected:
            selected.setText(self.newton_column_input.text().strip())
    
    def add_newton_column(self):
        # value = self.newton_column_input.text().strip()
        
        # if not value:
        #     value = "<New Column>"

        self.newton_columns_list.addItem("<New Column>")
        
        # Select added column
        last_index = self.newton_columns_list.count() - 1
        self.newton_columns_list.setCurrentRow(last_index)

    def remove_newton_column(self):
        for item in self.newton_columns_list.selectedItems():
            self.newton_columns_list.takeItem(self.newton_columns_list.row(item)) 
    
    def move_newton_column_up(self):
        row = self.newton_columns_list.currentRow()

        if row <= 0:
            return

        item = self.newton_columns_list.takeItem(row)
        self.newton_columns_list.insertItem(row - 1, item)
        self.newton_columns_list.setCurrentRow(row - 1)
        
    def move_newton_column_down(self):
        row = self.newton_columns_list.currentRow()

        if row < 0 or row >= self.newton_columns_list.count() - 1:
            return

        item = self.newton_columns_list.takeItem(row)
        self.newton_columns_list.insertItem(row + 1, item)
        self.newton_columns_list.setCurrentRow(row + 1)
    # ---- End Newton Columns ----

    # ---- Schema Map ----
    def build_schema_map_ui(self):
        self.dynamic_container.addWidget(QLabel("Schema Map"))
        
        self.schema_map_selector = QComboBox()

        for key in self.config.get("schema_map", {}).keys():
            display_text = key.replace("_", " ")
            self.schema_map_selector.addItem(display_text, key)
            
        self.schema_map_selector.currentTextChanged.connect(self.load_schema_map_list)
        self.dynamic_container.addWidget(self.schema_map_selector)

        self.schema_map_list = QListWidget()
        self.dynamic_container.addWidget(self.schema_map_list)
        self.schema_map_list.itemSelectionChanged.connect(self.load_schema_map)
        
        btn_row = QHBoxLayout()
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self.add_schema_map)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_schema_map)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)
        self.dynamic_container.addLayout(btn_row)

        schema_map_label_row = QHBoxLayout()
        schema_map_label_row.addWidget(QLabel("PCMS Column"))
        schema_map_label_row.addWidget(QLabel("Newton Column"))
        self.dynamic_container.addLayout(schema_map_label_row)

        schema_map_input_row = QHBoxLayout()
        self.pcms_column_selector = QComboBox()
        self.pcms_column_selector.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.pcms_column_selector.currentTextChanged.connect(self.load_schema_map)
        self.newton_column_selector = QComboBox()
        self.newton_column_selector.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        schema_map_input_row.addWidget(self.pcms_column_selector)
        schema_map_input_row.addWidget(self.newton_column_selector)
        self.dynamic_container.addLayout(schema_map_input_row)
        self.newton_column_selector.currentTextChanged.connect(self.update_schema_map_list)
        
        # Load first schema_map automatically
        self.load_schema_map_list()

    def load_schema_map(self):
        value = None
        source = self.sender()
        
        if source == self.schema_map_list:
            selected = self.schema_map_list.currentItem()
            
            if not selected:
                return
            
            key, value = [x.strip() for x in selected.text().split("→")]
            index = self.pcms_column_selector.findText(str(key))
        
            if index >= 0:
                self.pcms_column_selector.setCurrentIndex(index)
                
        elif source == self.pcms_column_selector:
            self.schema_map_list.setCurrentItem(None)
            key = self.pcms_column_selector.currentText()
            
            for i in range(self.schema_map_list.count()):
                item_text = self.schema_map_list.item(i).text()
                parts = item_text.split("→")
                
                if len(parts) != 2:
                    return
            
                if key == parts[0].strip():
                    self.schema_map_list.setCurrentRow(i)
                    value = parts[1].strip()
                    break  
        
        if value:
            index = self.newton_column_selector.findText(str(value))
            
            if index >= 0:
                self.newton_column_selector.setCurrentIndex(index)
        else:
            self.newton_column_selector.setCurrentIndex(-1)
            
    def load_schema_map_list(self):
        self.loaded = False
        schema_map = self.schema_map_selector.currentData()
        self.schema_map_list.clear()
        self.pcms_column_selector.clear()
        self.newton_column_selector.clear()
        
        schema_map_dict = self.config.get("schema_map", {}).get(schema_map, {})
        
        for key, value in schema_map_dict.items():
            self.schema_map_list.addItem(f"{key}  →  {value}")
        
        for builder in BUILDERS:
            for ws in builder.WORKSHEETS:
                if ws["key"] == schema_map:
                    sql = ws["sql"]
                
        pcms_columns = list(dict.fromkeys(get_sql_columns(sql)))
        
        for col in pcms_columns:
            self.pcms_column_selector.addItem(col)
        
        for col in self.config.get("newton_columns", {}).get(schema_map, []):
            self.newton_column_selector.addItem(col)
            
        self.schema_map_list.clearSelection()
        self.pcms_column_selector.setCurrentIndex(-1)
        self.newton_column_selector.setCurrentIndex(-1)
        self.loaded = True
        
    def update_schema_map_list(self):
        if not self.loaded:
            return
        
        selected = self.schema_map_list.currentItem()
        
        if not selected:
            return
        
        key = self.pcms_column_selector.currentText()
        value = self.newton_column_selector.currentText()
        selected.setText(f"{key}  →  {value}")
        
    def add_schema_map(self):
        key = self.pcms_column_selector.currentText().strip()
        value = self.newton_column_selector.currentText().strip()

        if not key and not value:
            QMessageBox.warning(self, "Invalid", "PCMS & Newton Columns cannot be empty")
            return
            
        self.schema_map_list.addItem(f"{key}  →  {value}")
        
        # Select added default_value
        last_index = self.schema_map_list.count() - 1
        self.schema_map_list.setCurrentRow(last_index)
        
    def remove_schema_map(self):
        for item in self.schema_map_list.selectedItems():
            self.schema_map_list.takeItem(self.schema_map_list.row(item))  
    # ---- End Schema Map ----

    def test(self):
        if validate_sql_engine(self.engine):
            QMessageBox.information(self, "Success", "SQL connection successful")
        else:
            QMessageBox.critical(self, "Failed", "SQL connection failed")
            
        return
        
    def save(self, silent=False):
        section = self.section_selector.currentText()
 
        if section == "Unit Filter":
            units = [
                self.unit_list.item(i).text().strip()
                for i in range(self.unit_list.count())
            ]

            self.config.setdefault("filters", {})["units"] = units
            
        elif section == "Mappings":
            mapping = self.mapping_selector.currentData()
            new_mapping = {}

            for i in range(self.mapping_list.count()):
                item_text = self.mapping_list.item(i).text()

                parts = item_text.split("→")
                if len(parts) != 2:
                    continue

                key = parts[0].strip()
                value = parts[1].strip()

                new_mapping[key] = value
                
            self.config["mappings"][mapping] = new_mapping
            
        elif section == "Default Values":
            defaults = self.default_value_selector.currentData()
            new_default_values = {}

            for i in range(self.default_value_list.count()):
                item_text = self.default_value_list.item(i).text()

                parts = item_text.split("→")
                if len(parts) != 2:
                    continue

                key = parts[0].strip()
                value = parts[1].strip()

                new_default_values[key] = value
            self.config["default_values"][defaults] = new_default_values
            
        elif section == "Newton Columns":
            newton_columns = self.newton_columns_selector.currentData()
            
            new_newton_columns = [
                self.newton_columns_list.item(i).text().strip()
                for i in range(self.newton_columns_list.count())
            ]

            self.config.setdefault("newton_columns", {})[newton_columns] = new_newton_columns
            
        elif section == "Schema Map":
            schema_map = self.schema_map_selector.currentData()
            new_schema_map = {}

            for i in range(self.schema_map_list.count()):
                item_text = self.schema_map_list.item(i).text()

                parts = item_text.split("→")
                if len(parts) != 2:
                    continue

                key = parts[0].strip()
                value = parts[1].strip()

                new_schema_map[key] = value
                self.config["schema_map"][schema_map] = new_schema_map
                
        # Connection
        self.config["connection"]["server"] = self.server_input.text().strip()
        self.config["connection"]["database"] = self.database_input.text().strip()
        # End Connection
        
        try:
            save_config(self.config)
        except Exception as e:
            QMessageBox.critical(self, "Save Failed", str(e))
            return
        
        if not silent:
            QMessageBox.information(self, "Success", "Config saved successfully")
        
        self.config = load_config()
        self.engine = get_engine(self.config)
        return
            
    def get_radio_option(self) -> str:
            return "post" if self.post_radio.isChecked() else "pre"
    
    def handle_radio(self):
        state = self.get_radio_option()
        self.config = apply_aimi_state(self.config, state)
        
        try:
            save_config(self.config)
        except Exception as e:
            return
        
    def run_builder(self, choice):
        self.config = load_config()
        self.engine = get_engine(self.config)
        state = self.get_radio_option()

        if not validate_sql_engine(self.engine):
            QMessageBox.warning(self, "Connection Error", "SQL connection failed")
            return
        
        if sql_server_busy(self.engine):
            QMessageBox.warning(self, "SQL Busy", "SQL Server currently busy")
            return
        
        try:
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            
            wb = WORKBOOKS[choice]
            output_file = output_dir / f"{wb['filename']}_{timestamp}.xlsx"

            with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                wb["builder"](writer, self.engine, self.config, state)

            QMessageBox.information(self, "Success", f"{wb['filename']} workbook created successfully.")

        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def run_asset_register(self):
        self.run_builder("asset_register")

    def run_inspection_data(self):
        self.run_builder("inspection_data")

    def run_asset_task(self):
        self.run_builder("asset_task")

    def run_pof_assessment(self):
        self.run_builder("pof_assessment")

    def run_cof_assessment(self):
        self.run_builder("cof_assessment")

    def run_all(self):
        self.config = load_config()
        self.engine = get_engine(self.config)
        state = self.get_radio_option()
        
        if not validate_sql_engine(self.engine):
            QMessageBox.warning(self, "Connection Error", "SQL connection failed")
            return
        
        if sql_server_busy(self.engine):
            QMessageBox.warning(self, "SQL Busy", "SQL Server currently busy")
            return
        
        try:
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            for name, wb in WORKBOOKS.items():
                output_file = output_dir / f"{wb['filename']}_{timestamp}.xlsx"

                with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                    wb["builder"](writer, self.engine, self.config, state)

            QMessageBox.information(self, "Success", "All workbooks created.")

        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = QtWindow()
    win.show()
    sys.exit(app.exec())
