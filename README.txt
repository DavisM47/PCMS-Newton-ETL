PCMS → Newton ETL Tool

## Overview
- Graphical User Interface (PySide6)
- Config editor
- Mapping editor
- Unit filter editor
- SQL Server connection editor
- One-click ETL execution buttons

## Requirements
- Python 3.10+
- Access to PCMS database
- ODBC driver installed

## Setup

1. Install Python:
   https://www.python.org/downloads/

2. Create a virtual environment:
   python -m venv venv

3. Activate the environment:
   Windows:
     venv\Scripts\activate
   macOS/Linux:
     source venv/bin/activate

4. Install dependencies:
   pip install -r requirements.txt

## Run

From the project root:
   python -m ui.etl_app

