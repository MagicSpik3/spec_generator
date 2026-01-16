import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder

class TestTopologyBridge:
    
    @pytest.mark.parametrize("scenario, save_syntax, join_syntax", [
        # Scenario 1: Both Quoted (Standard SPSS)
        ("Both Quoted", "SAVE OUTFILE='temp.sav'.", "MATCH FILES /FILE=* /TABLE='temp.sav' /BY id."),
        
        # Scenario 2: Both Unquoted (Allowed in SPSS for simple filenames)
        ("Both Unquoted", "SAVE OUTFILE=temp.sav.", "MATCH FILES /FILE=* /TABLE=temp.sav /BY id."),
        
        # Scenario 3: Mixed (The Edge Case)
        ("Mixed Quotes", "SAVE OUTFILE='temp.sav'.", "MATCH FILES /FILE=* /TABLE=temp.sav /BY id."),
    ])
    def test_bridge_connectivity(self, scenario, save_syntax, join_syntax):
        """
        Verifies that the link between SAVE and JOIN persists regardless of quoting style.
        """
        print(f"\nTesting Scenario: {scenario}")
        
        # 1. Construct Script
        script = f"""
        DATA LIST FREE / id.
        {save_syntax}
        {join_syntax}
        """
        
        # 2. Parse & Build
        parser = SpssParser()
        ast = parser.parse(script)
        
        builder = GraphBuilder()
        pipeline = builder.build(ast)
        
        # 3. Diagnostics
        save_op = next((op for op in pipeline.operations if op.type == "save_binary"), None)
        join_op = next((op for op in pipeline.operations if op.type == "join"), None)
        
        assert save_op, "Critical: Save Op was not created"
        assert join_op, "Critical: Join Op was not created"

        print(f"  💾 SAVE Output ID: {save_op.outputs[0]}")
        print(f"  🔗 JOIN Input IDs: {join_op.inputs}")

        # 4. The Assertion
        # We check if the ID produced by Save exists in the Inputs of Join
        # We don't care *what* the ID is, only that they match.
        saved_id = save_op.outputs[0]
        
        # We expect the join to have 2 inputs: [active_dataset, saved_file]
        assert len(join_op.inputs) >= 2, \
            f"❌ Bridge Failed: Join only has {len(join_op.inputs)} inputs (Missed the file link)"
            
        assert saved_id in join_op.inputs, \
            f"❌ Mismatch: Join is looking for inputs {join_op.inputs}, but Save produced '{saved_id}'"