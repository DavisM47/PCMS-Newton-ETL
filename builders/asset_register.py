from builders.worksheet import build_worksheet
from builders.post_process import (
    areas_post_process,
    units_post_process,
    systems_post_process,
    sub_systems_post_process,
    assets_post_process,
    components_post_process
)

WORKSHEETS = [
    {
        "sql": """SELECT
                    dbo.complex.complex_seqno, dbo.complex.name AS complex, dbo.unit.unit_seqno
                    FROM dbo.complex 
                    INNER JOIN dbo.unit ON dbo.complex.complex_seqno = dbo.unit.complex_seqno""",
        "sheet": "AR Areas",
        "key": "areas",
        "post": lambda df, config, state: areas_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.unit.unit_seqno, dbo.unit.unit_id, dbo.unit.name, dbo.unit.complex_seqno
                    FROM dbo.unit""",
        "sheet": "AR Units",
        "key": "units",
        "post": lambda df, config, state: units_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type, dbo.system.system_seqno, dbo.system.system_code, dbo.unit.complex_seqno
                    FROM dbo.equip_type 
                    INNER JOIN dbo.equip 
                    INNER JOIN dbo.unit ON dbo.equip.unit_seqno = dbo.unit.unit_seqno ON dbo.equip_type.equip_type_seqno = dbo.equip.equip_type_seqno 
                    LEFT OUTER JOIN dbo.system 
                    INNER JOIN dbo.circuit ON dbo.system.system_seqno = dbo.circuit.system_seqno ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    WHERE (dbo.equip.history_no = 0)""",
        "sheet": "AR Systems",
        "key": "systems",
        "post": lambda df, config, state: systems_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type, dbo.system.system_seqno, dbo.system.system_code, dbo.unit.complex_seqno
                    FROM dbo.equip_type 
                    INNER JOIN dbo.equip 
                    INNER JOIN dbo.unit ON dbo.equip.unit_seqno = dbo.unit.unit_seqno ON dbo.equip_type.equip_type_seqno = dbo.equip.equip_type_seqno 
                    LEFT OUTER JOIN dbo.system 
                    INNER JOIN dbo.circuit ON dbo.system.system_seqno = dbo.circuit.system_seqno ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    WHERE (dbo.equip.history_no = 0)""",
        "sheet": "AR Sub-Systems",
        "key": "sub_systems",
        "post": lambda df, config, state: sub_systems_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_id, dbo.equip.name, 
                    dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type, dbo.equip.install_dt, dbo.equip.in_service_dt, dbo.equip.build_dt, 
                    dbo.operating_status.name AS operating_status, dbo.unit.install_dt AS unit_install_dt, dbo.system.system_seqno, dbo.system.system_code
                    FROM dbo.system 
                    INNER JOIN dbo.circuit ON dbo.system.system_seqno = dbo.circuit.system_seqno 
                    RIGHT OUTER JOIN dbo.equip 
                    INNER JOIN dbo.operating_status ON dbo.equip.operating_status_seqno = dbo.operating_status.operating_status_seqno 
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno 
                    INNER JOIN dbo.unit ON dbo.equip.unit_seqno = dbo.unit.unit_seqno ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    WHERE (dbo.equip.history_no = 0)""",
        "sheet": "AR Assets",
        "key": "assets",
        "post": lambda df, config, state: assets_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, dbo.equip_type.name AS equip_type,
                    dbo.circuit.circuit_seqno, dbo.circuit.circuit_id, dbo.circuit.name,
                    dbo.system.system_seqno, dbo.system.system_code
                    FROM dbo.equip 
                    INNER JOIN dbo.circuit ON dbo.equip.equip_seqno = dbo.circuit.equip_seqno
                    INNER JOIN dbo.equip_type ON dbo.equip.equip_type_seqno = dbo.equip_type.equip_type_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE dbo.equip.history_no = 0 AND dbo.circuit.history_no = 0""",
        "sheet": "AR Components",
        "key": "components",
        "post": lambda df, config, state: components_post_process(df, config, state),
        "clean": True,
    }
]

def build_asset_register(writer, engine, config, state):
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
