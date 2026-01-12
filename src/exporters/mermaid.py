from src.ir.model import Pipeline
from src.ir.types import OpType

class MermaidExporter:
    def export(self, pipeline: Pipeline) -> str:
        lines = ["graph TD"]
        
        # 1. Define Styles
        lines.append("    classDef dataset fill:#e1f5fe,stroke:#01579b,stroke-width:2px,rx:5,ry:5;")
        lines.append("    classDef op_load fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px;")
        lines.append("    classDef op_save fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px;")
        lines.append("    classDef op_compute fill:#bbdefb,stroke:#0d47a1,stroke-width:2px;")
        lines.append("    classDef op_filter fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;")
        lines.append("    classDef op_agg fill:#ffccbc,stroke:#d84315,stroke-width:2px;")
        lines.append("    classDef op_join fill:#e1bee7,stroke:#4a148c,stroke-width:2px;")
        lines.append("    classDef op_generic fill:#f5f5f5,stroke:#616161,stroke-width:2px,stroke-dasharray: 5 5;")

        # 2. Define Nodes
        # Datasets are Cylinders (Database shape in Mermaid is [("label")])
        for ds in pipeline.datasets:
            # Shorten ID for display
            label = ds.id.replace("source_", "").replace("ds_", "").replace("_derived", "")
            lines.append(f'    {ds.id}[("{label}")]:::dataset')

        # Operations are Boxes
        for op in pipeline.operations:
            style = self._get_style(op.type)
            label = self._get_label(op)
            lines.append(f'    {op.id}["{label}"]:::{style}')
            
            # 3. Define Edges (Data Lineage)
            # Input -> Op
            for inp in op.inputs:
                lines.append(f"    {inp} --> {op.id}")
            
            # Op -> Output
            for out in op.outputs:
                lines.append(f"    {op.id} --> {out}")

        return "\n".join(lines)

    def _get_style(self, op_type: OpType) -> str:
        if op_type == OpType.LOAD: return "op_load"
        if op_type == OpType.SAVE: return "op_save"
        if op_type == OpType.COMPUTE: return "op_compute"
        if op_type == OpType.FILTER: return "op_filter"
        if op_type == OpType.AGGREGATE: return "op_agg"
        if op_type == OpType.JOIN: return "op_join"
        return "op_generic"

    def _get_label(self, op) -> str:
        # Smart labels based on context
        if op.type == OpType.COMPUTE:
            return f"COMPUTE<br/>{op.params.get('target')} = ..."
        if op.type == OpType.FILTER:
            return f"FILTER<br/>{op.params.get('condition')}"
        if op.type == OpType.AGGREGATE:
            return f"AGGREGATE<br/>By: {op.params.get('break')}"
        if op.type == OpType.JOIN:
            return f"JOIN<br/>On: {op.params.get('by')}"
        if op.type == OpType.GENERIC:
            return f"GENERIC<br/>{op.params.get('command')}"
        return op.type.value.upper()