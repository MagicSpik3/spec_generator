import pytest
import os
from src.importers.spss.parser import SpssParser
from src.importers.spss.graph_builder import GraphBuilder
from src.exporters.yaml import IrYamlExporter

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
        assert "id: ds_input.csv" in content
        assert "type: load_csv" in content
        assert "type: compute_columns" in content
        assert "type: save_binary" in content