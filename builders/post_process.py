import pandas as pd
from datetime import date

def resolve_col(schema_map: dict, source_key: str, fallback: str = None) -> str:
    """Return the mapped Newton column name for a PCMS source key, or fall back to the key itself."""
    return schema_map.get(source_key, fallback or source_key)

#  ---- Asset Register
def areas_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("areas", {})
    
    complex = resolve_col(schema_map_dict, "complex_seqno")
        
    df["Area Client ID"] =  df[complex] 
        
    return df

def units_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("units", {})
    
    complex = resolve_col(schema_map_dict, "complex_seqno")
    unit = resolve_col(schema_map_dict, "unit_seqno")
    
    df["Unit Client ID"] =  df[complex] + '-' + df[unit]
    df["Area Client ID"] =  df[complex]
    
    # climate_type_map = config["mappings"]["climate"]
    # df["Climate"] = df["Climate"].map(climate_type_map).fillna(df["Climate"])
    
    return df

def systems_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("systems", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    
    complex = resolve_col(schema_map_dict, "complex_seqno")
    unit = resolve_col(schema_map_dict, "unit_seqno")
    sys = resolve_col(schema_map_dict, system_mode)
        
    df["System Client ID"] =  df[unit] + '-' + df[sys]
    df["Unit Client ID"] =  df[complex] + '-' + df[unit]

    return df

def sub_systems_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("sub_systems", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    unit = resolve_col(schema_map_dict, "unit_seqno")
    sys = resolve_col(schema_map_dict, system_mode)
    
    df["Sub-System Client ID"] = df[unit] + '-' + df[sys] + "-sub"
    df["System Client ID"] =  df[unit] + '-' + df[sys]

    return df

def assets_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("assets", {})

    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    
    sys = resolve_col(schema_map_dict, system_mode)
    unit = resolve_col(schema_map_dict, "unit_seqno")
    equip = resolve_col(schema_map_dict, "equip_seqno")

    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    df["Sub-System Client ID"] = df[unit] + '-' + df[sys] + "-sub"

    unit_dt = resolve_col(schema_map_dict, "unit_install_dt")
    build_dt = resolve_col(schema_map_dict, "build_dt")
    install_dt = resolve_col(schema_map_dict, "install_dt")
    in_service_dt = resolve_col(schema_map_dict, "in_service_dt")

    # Convert to datetime
    df[unit_dt] = pd.to_datetime(df[unit_dt], errors="coerce")
    df[build_dt] = pd.to_datetime(df[build_dt], errors="coerce")
    df["Install Date"] = pd.to_datetime(df["Install Date"], errors="coerce")
    df["In Service Date"] = pd.to_datetime(df["In Service Date"], errors="coerce")
    
    df[build_dt] = df[build_dt].fillna(df[unit_dt])
    df["Install Date"] = df["Install Date"].fillna(df[build_dt])
    df["Install Date"] = df["Install Date"].fillna(df[in_service_dt])
    df["In Service Date"] = df["In Service Date"].fillna(df[install_dt])

    # Find earliest date across both columns
    earliest_date = pd.concat([df["Install Date"], df["In Service Date"]]).min()
    mask = df["Install Date"].isna() & df["In Service Date"].isna()
    df.loc[mask, ["Install Date", "In Service Date"]] = earliest_date

    mask = df["In Service Date"].notna() & (df["In Service Date"] < df["Install Date"])
    df.loc[mask, "In Service Date"] = df["Install Date"]

    today = pd.Timestamp.today().normalize()
    mask = (df["Install Date"] > today) & (df["In Service Date"] < today)
    df.loc[mask, "Install Date"] = df["In Service Date"]

    mask = (df["In Service Date"] > today) & (df["Install Date"] < today)
    df.loc[mask, "In Service Date"] = df["Install Date"]
    
    # Format dates
    df["Install Date"] = pd.to_datetime(df["Install Date"]).dt.strftime("%m/%d/%Y")
    df["In Service Date"] = pd.to_datetime(df["In Service Date"]).dt.strftime("%m/%d/%Y")
    
    equip_type = resolve_col(schema_map_dict, "equip_type")
    status = resolve_col(schema_map_dict, "operating_status")
        
    equip_type_map = config["mappings"]["equip_type"]
    df[equip_type] = df[equip_type].map(equip_type_map).fillna(df[equip_type])
    
    operating_status_map = config["mappings"]["operating_status"]
    df[status] = df[status].map(operating_status_map).fillna(df[status])

    return df

def components_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("components", {})

    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    
    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    return df

#  ---- Inspection Data
def thickness_locations_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("thickness_locations", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    tml = resolve_col(schema_map_dict, "tml_seqno")
    ext_id = resolve_col(schema_map_dict, "tml_id")
    size = resolve_col(schema_map_dict, "pipe_size")
    nominal = resolve_col(schema_map_dict, "nominal_thickness")
    inner_diameter = resolve_col(schema_map_dict, "inner_diameter")
    outer_diameter = resolve_col(schema_map_dict, "outer_diameter")
    orig_thickness = resolve_col(schema_map_dict, "orig_thickness")
    ret_limit = resolve_col(schema_map_dict, "retiring_limit")
    category = resolve_col(schema_map_dict, "category")
    initial_reading = resolve_col(schema_map_dict, "initial_reading")
    
    client_ids = df[ext_id]
    external_ids = df[circuit] + '-' + df[tml]
    
    df["CML Client ID"] = client_ids
    df["External Source ID"] = external_ids
    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]

    # Convert first to create NaNs & Fill NaNs with 0
    df[size] = pd.to_numeric(df[size], errors='coerce').fillna(0).astype(float)
    df[nominal] = pd.to_numeric(df[nominal], errors='coerce').fillna(0).astype(float)
    df[inner_diameter] = pd.to_numeric(df[inner_diameter], errors='coerce').fillna(0).astype(float)
    df[outer_diameter] = pd.to_numeric(df[outer_diameter], errors='coerce').fillna(0).astype(float)
    df[orig_thickness] = pd.to_numeric(df[orig_thickness], errors='coerce').fillna(0).astype(float)
    df[ret_limit] = pd.to_numeric(df[ret_limit], errors='coerce').fillna(0).astype(float)
    df[initial_reading] = pd.to_numeric(df[initial_reading], errors='coerce').fillna(0).astype(float)

    # mask = (df[inner_diameter] == 0) & (df[size] > 0)
    # df.loc[mask, inner_diameter] = df[size]

    mask = df[nominal] == 0
    df.loc[mask, nominal] = df[initial_reading]
    
    mask = df[orig_thickness] == 0
    df.loc[mask, orig_thickness] = df[nominal]

    mask = (df[orig_thickness] == 0) & (df[outer_diameter] > 0) & (df[inner_diameter] > 0)
    df.loc[mask, orig_thickness] = (df[outer_diameter] - df[inner_diameter]) / 2

    mask = (df[orig_thickness] == 0) & (df[ret_limit] == 0)
    df.loc[
        mask, [orig_thickness, ret_limit, "Note"]
        ] = [0.5, 0.375, "Assumption: 0.5in used for Thickness & 0.125in used for CA"]
    
    mask = (df[orig_thickness] == 0) & (df[ret_limit] > 0)
    df.loc[mask, orig_thickness] = df[ret_limit] + 0.125
    df.loc[mask, "Note"] = "Assumption: 0.125in used for Corrosion Allowance"
    
    mask = (df[ret_limit] == 0) & (df[orig_thickness] > 0)
    df.loc[mask, ret_limit] = df[orig_thickness] - 0.125
    df.loc[mask, "Note"] = "Assumption: 0.125in used for Corrosion Allowance"
    
    mask = (df[ret_limit] > df[orig_thickness])
    df.loc[mask, ret_limit] = df[orig_thickness] - 0.125
    df.loc[mask, "Note"] = "Assumption: 0.125in used for Corrosion Allowance"
    
    mask = (df[outer_diameter] == 0) & (df[orig_thickness] > 0) & (df[inner_diameter] > 0)
    df.loc[mask, outer_diameter] = df[inner_diameter] + (df[orig_thickness] * 2)

    mask = (df[inner_diameter] == 0) & (df[orig_thickness] > 0) & (df[outer_diameter] > 0)
    df.loc[mask, inner_diameter] = df[outer_diameter] - (df[orig_thickness] * 2)

    mask = (df[outer_diameter] == 0) & (df[inner_diameter] == 0)
    df.loc[mask, outer_diameter] = 10
    df.loc[mask, inner_diameter] = (df[outer_diameter]) - (df[orig_thickness] * 2)

    df["Inside Diameter"] = pd.to_numeric(df[inner_diameter], errors='coerce').fillna(0)
    df["Outside Diameter"] = pd.to_numeric(df[outer_diameter], errors='coerce').fillna(0)
    df["Base Material Thickness"] = pd.to_numeric(df[orig_thickness], errors='coerce').fillna(0)
    df["Retirement Limit"] = pd.to_numeric(df[ret_limit], errors='coerce').fillna(0)
    df["Critical Limit"] = pd.to_numeric(df[ret_limit], errors='coerce').fillna(0)

    df["Inside Diameter"] = df["Inside Diameter"].astype(object)
    df.loc[df["Inside Diameter"]  == 0, "Inside Diameter"] = ""
    df["Outside Diameter"] = df["Outside Diameter"].astype(object)
    df.loc[df["Outside Diameter"]  == 0, "Outside Diameter"] = ""
    df["Base Material Thickness"] = df["Base Material Thickness"].astype(object)
    df.loc[df["Base Material Thickness"]  == 0, "Base Material Thickness"] = ""
    df["Retirement Limit"] = df["Retirement Limit"].astype(object)
    df.loc[df["Retirement Limit"]  == 0, "Retirement Limit"] = ""
    df["Critical Limit"] = df["Critical Limit"].astype(object)
    df.loc[df["Critical Limit"]  == 0, "Critical Limit"] = ""

    category_map = config["mappings"]["category"]
    df[category] = df[category].map(category_map).fillna(df[category])
    
    return df

def thickness_readings_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("thickness_readings", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    ext_id = resolve_col(schema_map_dict, "tml_id")
    reading = resolve_col(schema_map_dict, "thickness")
    read_dt = resolve_col(schema_map_dict, "read_dt")
    quality_ind = resolve_col(schema_map_dict, "quality_ind")
    
    df["CML Client ID"] = df[ext_id]
    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]

    df[reading] = pd.to_numeric(df[reading], errors='coerce').fillna(0)
    df["Reading"] = df[reading].astype(object)
    df.loc[df["Reading"]  == 0, "Reading"] = ""
        
    df[read_dt] = pd.to_datetime(df[read_dt], errors="coerce")
    df[read_dt] = pd.to_datetime(df[read_dt]).dt.strftime("%m/%d/%Y")
    
    quality_ind_map = config["mappings"]["quality_ind"]
    df[quality_ind] = df[quality_ind].replace(r"^\s*$", pd.NA, regex=True).map(quality_ind_map).fillna("FALSE")
    
    return df

# ---- Asset Tasks
def tasks_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("tasks", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    thin_type = resolve_col(schema_map_dict, "CGL_THIN")

    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    df.loc[(df[thin_type] == "General"), "Damage Mode"] = "General Thinning"
    df.loc[(df[thin_type] == "Local"), "Damage Mode"] = "Local Thinning"
    
    return df

def steps_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("steps", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    downtime = resolve_col(schema_map_dict, "estimated_repair_time")
    cost = resolve_col(schema_map_dict, "USER_FCOST_MAX")
    
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    if state == "pre":
        return df
    
    df[downtime] = pd.to_numeric(df[downtime], errors='coerce').fillna(0)
    
    df["Downtime (Hours)"] = df[downtime] * 24
    df["Material Cost ($)"] = df[cost]
    
    df = df.sort_values("Material Cost ($)", ascending=False).drop_duplicates(subset="Asset Client ID", keep='first')
    
    return df

#  ---- POF Assessment
def assessments_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("assessments", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    df["Status Changed"] = date.today().strftime("%m/%d/%Y")
    
    return df

def functions_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("functions", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    return df

def failure_modes_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("failure_modes", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    suggest_rate = resolve_col(schema_map_dict, "suggest_rate")
    damage_mode = resolve_col(schema_map_dict, "damage_mode")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    if state == "pre":
        return df
    
    cols_to_clear = [
        "Name",
        "PoF Model",
        "Base Material Corrosion Rate (Mils/Year)",
    ]

    mask = df[damage_mode] != "Internal Loss of Thickness"
    df.loc[mask, cols_to_clear] = None

    df[suggest_rate] = pd.to_numeric(df[suggest_rate], errors='coerce').fillna(0)
    df["Base Material Corrosion Rate (Mils/Year)"] = df[suggest_rate] * 1000
    
    damage_mode_map = config["mappings"]["damage_mode"]
    df["Name"] = df[damage_mode].map(damage_mode_map).fillna(df[damage_mode])
    pof_model_map = config["mappings"]["pof_model"]
    df["PoF Model"] =  df["Name"].map(pof_model_map).fillna(df["Name"])
    
    return df

def failure_mechanisms_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("failure_mechanisms", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    damage_mode = resolve_col(schema_map_dict, "damage_mode")
    dm = resolve_col(schema_map_dict, "damage_mechanism")
    suggest_rate = resolve_col(schema_map_dict, "suggest_rate")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]

    if state == "pre":
        return df

    cols_to_clear = [
        "Name",
        "Min Modeled Corrosion Rate Override",
        "Max Modeled Corrosion Rate Override",
        "Location",
        "Damage Mode",
        "Susceptibility Override",
        "Hole Size Override",
    ]

    mask = df[damage_mode] != "Internal Loss of Thickness"
    df.loc[mask, cols_to_clear] = None

    mask = df[damage_mode] == "Internal Loss of Thickness"
    df[suggest_rate] = pd.to_numeric(df[suggest_rate], errors='coerce').fillna(0)
    df.loc[mask, "Susceptibility Override"] = "Corrosion Rate Modeled"
    
    # Map damage mechanism names first so we can look up uncertainty
    damage_mechanism_map = config["mappings"]["damage_mechanism"]
    df["Name"] = df[dm].map(damage_mechanism_map).fillna(df[dm])
    
    # Apply uncertainty % to min/max rates
    uncertainty = config.get("corrosion_rate_uncertainty", {})
    base_rate = df.loc[mask, suggest_rate] * 1000
    
    min_uncertainty = df.loc[mask, "Name"].map(lambda name: uncertainty.get(name, {}).get("min", 0)).astype(float)
    max_uncertainty = df.loc[mask, "Name"].map(lambda name: uncertainty.get(name, {}).get("max", 0)).astype(float)

    df.loc[mask, "Min Modeled Corrosion Rate Override"] = base_rate * (1 - min_uncertainty)
    df.loc[mask, "Max Modeled Corrosion Rate Override"] = base_rate * (1 + max_uncertainty)

    df.loc[
        df[damage_mode] == "External Loss of Thickness", "Location" 
    ] = "External"
    
    df.loc[
        df[damage_mode] == "Internal Loss of Thickness", "Location" 
    ] = "Internal"
    
    df.loc[
        df[damage_mode] == "Environment Assisted Cracking", ["Location", "Damage Mode", "Hole Size Override"]
    ] = ["Internal", "Cracking", "Crack"]
    
    df.loc[
        df[damage_mode] == "Mechanical & Metallurgical Failure", ["Location", "Damage Mode", "Hole Size Override"]
    ] = ["Internal", "Cracking", "Crack"]

    damage_type_map = config["mappings"]["damage_type"]
    df["Damage Mode"] = df["damage_type"].map(damage_type_map).fillna(df["damage_type"])

    return df

def function_failure_mode_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("function_failure_mode", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    damage_mode = resolve_col(schema_map_dict, "damage_mode")
        
    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    if state == "pre":
        return df
    
    mask = df[damage_mode] != "Internal Loss of Thickness"
    df.loc[mask, "Failure Mode Name"] = None
    
    df.loc[
        df[damage_mode] == "Internal Loss of Thickness", "Failure Mode Name"
    ] = "Thinning"
        
    df.loc[
        df[damage_mode] == "External Loss of Thickness", "Failure Mode Name"
    ] = "Thinning"
        
    df.loc[
        df[damage_mode] == "Environment Assisted Cracking", "Failure Mode Name"
    ] = "Cracking/Metallurgical"
    
    df.loc[
        df[damage_mode] == "Mechanical & Metallurgical Failure", "Failure Mode Name"
    ] = "Cracking/Metallurgical"
    
    return df

def failure_mode_failure_mech_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("failure_mode_failure_mech", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    damage_mode = resolve_col(schema_map_dict, "damage_mode")
    dm = resolve_col(schema_map_dict, "damage_mechanism")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]

    if state == "pre":
        return df

    cols_to_clear = [
        "Failure Mode Name",
        "Failure Mechanism Name",
    ]

    mask = df[damage_mode] != "Internal Loss of Thickness"
    df.loc[mask, cols_to_clear] = None

    df.loc[
        df[damage_mode] == "Internal Loss of Thickness", "Failure Mode Name"
    ] = "Thinning"
        
    df.loc[
        df[damage_mode] == "External Loss of Thickness", "Failure Mode Name"
    ] = "Thinning"
        
    df.loc[
        df[damage_mode] == "Environment Assisted Cracking", "Failure Mode Name"
    ] = "Cracking/Metallurgical"
    
    df.loc[
        df[damage_mode] == "Mechanical & Metallurgical Failure", "Failure Mode Name"
    ] = "Cracking/Metallurgical"

    damage_mechanism_map = config["mappings"]["damage_mechanism"]
    df["Failure Mechanism Name"] = df[dm].map(damage_mechanism_map).fillna(df[dm])
    
    return df

#  ---- COF Assessment
def shared_fields_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("shared_fields", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    return df

def manual_hse_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("manual_hse", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")
    area = resolve_col(schema_map_dict, "AREA_CONS")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    if state == "post":
        df["Area (ft2)"] = df[area]
        
    return df

def newton_repair_replace_task_post_process(df, config, state):
    schema_map_dict = config.get("schema_map", {}).get("newton_repair_replace_task", {})
    
    system_mode = "system_seqno" if state == "post" else "equip_type_seqno"
    sys = resolve_col(schema_map_dict, system_mode)
    equip = resolve_col(schema_map_dict, "equip_seqno")
    circuit = resolve_col(schema_map_dict, "circuit_seqno")

    df["Component Client ID"] = df[equip] + '-' + df[circuit]
    df["Asset Client ID"] = df[sys] + '-' + df[equip]
    
    return df
