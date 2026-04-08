import pandas as pd
from utils.cleaning import clean_rtf, remove_illegal_excel_chars, normalize_dataframe
from utils.db import apply_unit_filter, validate_sql_engine

def build_worksheet(
    sql,
    sheet_name,
    writer,
    engine,
    config,
    state,
    key,
    post_process_fn=None,
    clean_text=False,
):
    sql = apply_unit_filter(sql, engine, config, key) # applies unit filter
    chunks = pd.read_sql(sql, engine, chunksize=50000)
    df = pd.concat(chunks)

    # cleans every column safely
    df = normalize_dataframe(df, key)

    if clean_text:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(clean_rtf).apply(remove_illegal_excel_chars)

    schema_map = config["schema_map"].get(key, {})
    df = df.rename(columns=schema_map)

    defaults = config.get("default_values", {}).get(key, {})
    for col, value in defaults.items():
        df[col] = value

    if post_process_fn:
        df = post_process_fn(df, config, state)

    newton_columns = list(config["newton_columns"][key])
    df = df.reindex(columns=newton_columns)
    
    df = df.drop_duplicates()
    
    df.to_excel(writer, sheet_name=sheet_name, index=False, freeze_panes=(1, 0))
