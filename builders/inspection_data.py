from builders.worksheet import build_worksheet
from builders.post_process import (
    thickness_locations_post_process,
    thickness_readings_post_process
)

WORKSHEETS = [
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, 
                    dbo.tml.circuit_seqno, dbo.circuit.system_seqno, dbo.system.system_code,
                    dbo.tml.tml_seqno, dbo.tml.tml_id, dbo.tml.keywords, dbo.tml.pipe_size, 
                    dbo.tml.nominal_thickness, dbo.tml.retiring_limit, dbo.tml.category, dbo.tml.initial_reading,
                    dbo.component.inner_diameter, dbo.component.outer_diameter, dbo.component.orig_thickness
                    FROM dbo.circuit 
                    INNER JOIN dbo.tml ON dbo.circuit.circuit_seqno = dbo.tml.circuit_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    LEFT OUTER JOIN dbo.component ON dbo.tml.component_seqno = dbo.component.component_seqno
                    WHERE (dbo.equip.history_no = 0) AND (dbo.circuit.history_no = 0) AND (dbo.tml.history_no = 0)""",
        "sheet": "Thickness Reading Locations",
        "key": "thickness_locations",
        "post": lambda df, config, state: thickness_locations_post_process(df, config, state),
        "clean": True,
    },
    {
        "sql": """SELECT 
                    dbo.equip.unit_seqno, dbo.equip.equip_seqno, dbo.equip.equip_type_seqno, 
                    dbo.circuit.circuit_seqno, dbo.circuit.system_seqno, dbo.system.system_code, 
                    dbo.tml.tml_seqno, dbo.tml.tml_id, dbo.reading.read_dt, dbo.reading.thickness, dbo.reading.quality_ind, 
                    dbo.reading.description
                    FROM dbo.circuit 
                    INNER JOIN dbo.tml ON dbo.circuit.circuit_seqno = dbo.tml.circuit_seqno 
                    INNER JOIN dbo.reading ON dbo.tml.tml_seqno = dbo.reading.tml_seqno 
                    INNER JOIN dbo.equip ON dbo.circuit.equip_seqno = dbo.equip.equip_seqno
                    INNER JOIN dbo.system ON dbo.circuit.system_seqno = dbo.system.system_seqno
                    WHERE (dbo.equip.history_no = 0) AND (dbo.circuit.history_no = 0) AND (dbo.tml.history_no = 0)""",
        "sheet": "Thickness Readings",
        "key": "thickness_readings",
        "post": lambda df, config, state: thickness_readings_post_process(df, config, state),
        "clean": True,
    }
]

def build_inspection_data(writer, engine, config, state):
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
            ws.get("clean", False),
        )
