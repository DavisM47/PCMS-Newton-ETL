import re
import pandas as pd
from striprtf.striprtf import rtf_to_text

def clean_rtf(text):
    if pd.isna(text):
        return text

    text = str(text)
    # If RTF, convert first
    if text.startswith('{'):
        try:
            text = rtf_to_text(text)
        except Exception:
            pass
    # ALWAYS strip whitespace
    return text.strip() 

def remove_illegal_excel_chars(val):
    if not isinstance(val, str):
        return val
    
    illegal_chars_re = re.compile(
        r"[\000-\010]|[\013-\014]|[\016-\037]|"
        r"[\x00-\x1f\x7f-\x84\x86-\x9f\ud800-\udfff\ufdd0-\ufdef\ufffe\uffff]"
    )
    
    return illegal_chars_re.sub("", val)

def normalize_dataframe(df, key):
    tml_keys = {"thickness_locations", "thickness_readings"}
    asset_keys = {"assets", "components"}
    
    for col in df.columns:
        if key in tml_keys and col == "tml_id":
            continue
        
        if key in asset_keys and col == "equip_id" or col == "circuit_id":
            continue
            
        series = df[col]

        # Try datetime first
        if pd.api.types.is_datetime64_any_dtype(series):
            df[col] = series.dt.strftime("%m/%d/%Y")
            continue
    
        # Try numeric conversion
        numeric = pd.to_numeric(series, errors="coerce")

        if numeric.notna().sum() > 0:
            df[col] = numeric.fillna(0).apply(lambda x: str(int(x)) if x.is_integer() else str(x))
        else:
            df[col] = series.astype(str).str.strip().replace("nan", "")

    return df
