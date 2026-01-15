import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.graph_builder import GraphBuilder
from src.exporters.yaml import IrYamlExporter
from etl_ir.types import DataType, OpType
from etl_ir.model import Column   

class TestEndToEnd:
    def test_full_pipeline_construction_and_export(self, tmp_path):
        """
        End-to-End: Raw SPSS -> Tokens -> AST -> IR Graph -> YAML Artifact
        """
        code = """
        GET DATA /TYPE=TXT /FILE='input.csv'.
        COMPUTE x = 1.
        SAVE OUTFILE='output.sav'.
        """
        
        # 1. Ingest
        parser = SpssParser()
        nodes = parser.parse(code)
        
        builder = GraphBuilder()
        pipeline = builder.build(nodes)
        
        # 2. Export
        exporter = IrYamlExporter()
        output_file = tmp_path / "pipeline_spec.yaml"
        exporter.export(pipeline, str(output_file))
        
        # 3. Verify Artifact
        assert output_file.exists()
        content = output_file.read_text()
        
        # Check for key structural elements in the YAML
        # Updated to match the new Deterministic GraphBuilder ID scheme
        assert "id: source_input.csv" in content
        assert "type: load_csv" in content
        assert "type: compute_columns" in content
        assert "target: x" in content