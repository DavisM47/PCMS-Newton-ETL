from builders.worksheet import build_worksheet
from builders.post_process import (
    assessments_post_process,
    functions_post_process,
    failure_modes_post_process,
    failure_mechanisms_post_process,
    function_failure_mode_post_process,
    failure_mode_failure_mech_post_process
)

pre_aimi_sql = """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.circuit_seqno
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0"""

WORKSHEETS = [
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.equip.equip_id, dbo.circuit.circuit_id,
                    dbo.circuit_damage_mech.circuit_seqno, dbo.damage_mechanism.name AS damage_mechanism, dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno 
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Assessments",
        "key": "assessments",
        "post": lambda df, config, state: assessments_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.circuit_damage_mech.circuit_seqno, dbo.circuit.circuit_id,
                    dbo.damage_mechanism.name AS damage_mechanism, dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno  
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Functions",
        "key": "functions",
        "post": lambda df, config, state: functions_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.circuit_damage_mech.circuit_seqno, dbo.circuit.circuit_id,
                    dbo.damage_mechanism.name AS damage_mechanism,  dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno 
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Failure Modes",
        "key": "failure_modes",
        "post": lambda df, config, state: failure_modes_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.circuit_damage_mech.circuit_seqno, dbo.circuit.circuit_id,
                    dbo.damage_mechanism.name AS damage_mechanism, dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno 
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Failure Mechanisms",
        "key": "failure_mechanisms",
        "post": lambda df, config, state: failure_mechanisms_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.circuit_damage_mech.circuit_seqno, dbo.circuit.circuit_id,
                    dbo.damage_mechanism.name AS damage_mechanism, dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno 
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Function - Failure Mode",
        "key": "function_failure_mode",
        "post": lambda df, config, state: function_failure_mode_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.circuit.equip_seqno, dbo.circuit_damage_mech.circuit_seqno, dbo.circuit.circuit_id,
                    dbo.damage_mechanism.name AS damage_mechanism, dbo.damage_type.damage_type, 
                    dbo.circuit_damage_mech.susceptible_yn, dbo.damage_mode.name AS damage_mode, dbo.circuit.suggest_rate,
                    dbo.circuit.system_seqno, dbo.system.system_code, dbo.equip.equip_type_seqno
                    FROM dbo.circuit 
                    INNER JOIN dbo.circuit_damage_mech ON dbo.circuit.circuit_seqno = dbo.circuit_damage_mech.circuit_seqno 
                    INNER JOIN dbo.damage_mechanism ON dbo.circuit_damage_mech.damage_mechanism_seqno = dbo.damage_mechanism.damage_mechanism_seqno 
                    INNER JOIN dbo.damage_type ON dbo.circuit_damage_mech.damage_type_seqno = dbo.damage_type.damage_type_seqno 
                    INNER JOIN dbo.damage_mode ON dbo.damage_mechanism.damage_mode_seqno = dbo.damage_mode.damage_mode_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno 
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno 
                    WHERE (dbo.circuit_damage_mech.susceptible_yn = 'Y') 
                    AND (dbo.damage_mechanism.name NOT IN ('Internal Loss of Thickness', 'Mechanical & Metallurgical Failure', 'Environment Assisted Cracking', 'External Loss of Thickness'))
                    AND dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Failure Mode - Failure Mech",
        "key": "failure_mode_failure_mech",
        "post": lambda df, config, state: failure_mode_failure_mech_post_process(df, config, state),
        "clean": True,
    }
]

def build_pof_assessment(writer, engine, config, state):
    for ws in WORKSHEETS:
        sql = ws["sql"] if state == "post" else pre_aimi_sql
        
        build_worksheet(
            sql,
            ws["sheet"],
            writer,
            engine,
            config,
            state,
            ws["key"],
            ws.get("post"),
            ws.get("clean", False)
        )
