import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from etl_ir.types import OpType

def test_sort_generation_flow():
    """
    Scenario: Load -> Sort -> Save
    Goal: Ensure the 'SORT' command generates a SORT_ROWS op with correct params.
    """
    script = """
    DATA LIST FREE / id.
    SORT CASES BY id.
    SAVE OUTFILE='sorted.sav'.
    """

    # 1. Parse
    parser = SpssParser()
    ast = parser.parse(script)

    # 2. Build
    builder = GraphBuilder()
    pipeline = builder.build(ast)

    # 3. Inspect Operations
    ops = pipeline.operations
    
    # Locate the Sort Op
    sort_op = next((op for op in ops if op.type == OpType.SORT_ROWS), None)
    
    # Assertions
    assert sort_op is not None, "Sort Operation (SORT_ROWS) was not created!"
    
    # Check Topology
    assert sort_op.inputs is not None
    assert len(sort_op.inputs) > 0, "Sort op is disconnected (no input)"
    
    # Check Parameters (Critical for the R crash 'unknown' error)
    # We expect 'id' to be in the keys, not 'unknown'
    keys = sort_op.parameters.get('keys', '')
    assert 'id' in keys, f"Expected sort key 'id', found '{keys}'"