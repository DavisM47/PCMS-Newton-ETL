from builders.worksheet import build_worksheet
from builders.post_process import (
    shared_fields_post_process,
    manual_hse_post_process,
    
    newton_repair_replace_task_post_process
)

WORKSHEETS = [
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.circuit_seqno
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Shared Fields",
        "key": "shared_fields",
        "post": lambda df, config, state: shared_fields_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.circuit_seqno, dbo.APIRBI_pcmsint_ext_output.AREA_CONS
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    INNER JOIN dbo.APIRBI_pcmsint_ext_output ON dbo.circuit.circuit_seqno = dbo.APIRBI_pcmsint_ext_output.circuit_seqno
                    WHERE dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Manual (HSE)",
        "key": "manual_hse",
        "post": lambda df, config, state: manual_hse_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.circuit_seqno
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "Newton Repair-Replace Task",
        "key": "newton_repair_replace_task",
        "post": lambda df, config, state: newton_repair_replace_task_post_process(df, config, state),
        "clean": True,
    }
]

def build_cof_assessment(writer, engine, config, state):
    for ws in WORKSHEETS:
        build_worksheet(
            ws["sql"],
            ws["sheet"],
            writer,
            engine,
            config,
            state,
            ws["key"],
            ws.get("post"),
            ws.get("clean", False)
        )
