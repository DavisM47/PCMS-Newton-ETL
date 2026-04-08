from pandas.io.formats import excel

def disable_default_header_style():
    excel.ExcelFormatter.header_style = None
