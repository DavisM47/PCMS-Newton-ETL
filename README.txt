PCMS → Newton ETL Tool

REQUIREMENTS
------------
Python 3.x
All dependencies listed in requirements.txt

Install dependencies:
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

