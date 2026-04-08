from builders.worksheet import build_worksheet
from builders.post_process import (
    tasks_post_process,
    steps_post_process
)

WORKSHEETS = [
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.estimated_repair_time,
                    dbo.APIRBI_pcmsint_ext_input.USER_FCOST_MAX, dbo.APIRBI_pcmsint_ext_input.CGL_THIN
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    LEFT OUTER JOIN dbo.APIRBI_pcmsint_ext_input ON dbo.circuit.circuit_seqno = dbo.APIRBI_pcmsint_ext_input.circuit_seqno
                    WHERE dbo.equip.history_no = 0""",
        "sheet": "Tasks",
        "key": "tasks",
        "post": lambda df, config, state: tasks_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.system.system_seqno, dbo.system.system_code, dbo.circuit.estimated_repair_time,
                    dbo.APIRBI_pcmsint_ext_input.USER_FCOST_MAX, dbo.APIRBI_pcmsint_ext_input.CGL_THIN
                    FROM  dbo.equip 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    LEFT OUTER JOIN dbo.APIRBI_pcmsint_ext_input ON dbo.circuit.circuit_seqno = dbo.APIRBI_pcmsint_ext_input.circuit_seqno
                    WHERE dbo.equip.history_no = 0""",
        "sheet": "Steps",
        "key": "steps",
        "post": lambda df, config, state: steps_post_process(df, config, state),
        "clean": True,
    }
]

def build_asset_task(writer, engine, config, state):
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
