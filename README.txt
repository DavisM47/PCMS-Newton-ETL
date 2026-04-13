PCMS → Newton ETL Tool

REQUIREMENTS
------------
Python 3.x
All dependencies listed in requirements.txt

VIRTUAL ENVIRONMENT
------------
A virtual environment keeps dependencies isolated from other Python projects on your system. 

Step 1 — Create the Virtual Environment
    Open a terminal in your project folder and run:
        py -m venv venv
    This creates a folder called venv/ inside your project directory.

Step 2 — Activate the Virtual Environment
    On Windows (PowerShell):
        venv\Scripts\activate
    Your terminal prompt will change to show (venv) when the environment is active.

Step 3 — Install Dependencies
    With the virtual environment active, install all required packages:
        pip install -r requirements.txt



CONFIGURATION
-------------
Update config.yaml with your SQL Server connection details before running.


RUNNING THE TOOL
----------------
Navigate to the project folder in your terminal.

Generate a single workbook:
    python main.py --workbook asset_register
    python main.py --workbook inspection_data
    python main.py --workbook asset_task
    python main.py --workbook pof_assessment
    python main.py --workbook cof_assessment

Generate all workbooks at once:
    python main.py --all

When prompted, enter "pre" or "post" for AIMI state.
If anything other than "pre" or "post" is entered, it will default to "pre".


OUTPUT
------
All generated Excel files are saved to the output/ folder
with a timestamp in the filename.

