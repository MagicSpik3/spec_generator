import yaml
from etl_ir.model import Pipeline, Dataset, Operation

class IrYamlExporter:
    def export(self, pipeline: Pipeline, output_path: str):
        data = self._to_dict(pipeline)
        
        with open(output_path, "w", encoding="utf-8") as f:
            # sort_keys=False preserves our logical ordering
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _to_dict(self, pipeline: Pipeline) -> dict:
        return {
            "metadata": {
                "generator": "SpecGen v0.1",
                "source_type": "SPSS"
            },
            "datasets": [
                {
                    "id": ds.id,
                    "source": ds.source,
                    "columns": [
                        {"name": col.name, "type": col.type.value} 
                        for col in ds.columns
                    ]
                }
                for ds in pipeline.datasets
            ],
            "operations": [
                {
                    "id": op.id,
                    "type": op.type.value,
                    "inputs": op.inputs,
                    "outputs": op.outputs,
                    "parameters": op.parameters
                }
                for op in pipeline.operations
            ]
        }