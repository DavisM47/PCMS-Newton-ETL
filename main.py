import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

from utils.db import create_sql_engine, validate_sql_engine, sql_server_busy
from utils.config_manager import load_config
from utils.excel import disable_default_header_style
from builders.asset_register import build_asset_register
from builders.inspection_data import build_inspection_data
from builders.asset_task import build_asset_task
from builders.pof_assessment import build_pof_assessment
from builders.cof_assessment import build_cof_assessment


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCMS data export tool"
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--workbook",
        choices=[
            "asset_register", 
            "inspection_data", 
            "asset_task", 
            "pof_assessment", 
            "cof_assessment"
            ],
        help="Generate a single workbook",
    )

    group.add_argument(
        "--all",
        action="store_true",
        help="Generate all available workbooks",
    )

    return parser.parse_args()

def main():
    args = parse_args()
    config = load_config()
    engine = create_sql_engine(config)
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    disable_default_header_style()

    if not validate_sql_engine(engine):
        print("Connection Error", "SQL connection failed")
        return
    
    if sql_server_busy(engine):
        print("SQL Busy", "SQL Server currently busy")
        return

    state = input("Pre or Post AIMI?: ")
    
    if state != "pre" and state != "post":
        state = "pre"
    
    if args.all:
        for name, wb in WORKBOOKS.items():
            output_file = output_dir / f"{wb['filename']}_{timestamp}.xlsx"

            with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                wb["builder"](writer, engine, config, state)

            print(f"Created: {output_file}")
    else:
        wb = WORKBOOKS[args.workbook]
        output_file = output_dir / f"{wb['filename']}_{timestamp}.xlsx"

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            wb["builder"](writer, engine, config, state)

        print(f"Created: {output_file}")

if __name__ == "__main__":
    main()
    