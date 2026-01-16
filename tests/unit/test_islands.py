import pytest
from spec_generator.importers.spss.graph_builder import GraphBuilder
from spec_generator.importers.spss.ast import SaveNode, JoinNode, LoadNode

def test_bridge_island_gap():
    """
    Scenario:
    1. We LOAD data (Island A).
    2. We SAVE it to 'temp.sav'.
    3. We LOAD other data (Island B).
    4. We JOIN 'temp.sav' into Island B.
    
    Expectation: The graph should be fully connected (1 component),
    because the JOIN should link back to step 2.
    """
    
    # 1. Setup the AST
    nodes = [
        # Island A
        LoadNode(filename="A.csv", file_type="csv", columns=[]),
        SaveNode(filename="temp.sav"),
        
        # Island B
        LoadNode(filename="B.csv", file_type="csv", columns=[]),
        # The critical link: Join uses the file we just saved
        JoinNode(sources=["*", "temp.sav"], by=["id"]) 
    ]
    
    # 2. Build
    builder = GraphBuilder()
    pipeline = builder.build(nodes)
    
    # 3. Analyze Topology
    # We can do a quick connectivity check by walking the ops
    
    # Find the Save Op
    save_op = next(op for op in pipeline.operations if op.type == "save_binary")
    save_output_id = save_op.outputs[0] # e.g., "file_temp.sav"
    
    # Find the Join Op
    join_op = next(op for op in pipeline.operations if op.type == "join")
    join_inputs = join_op.inputs
    
    print(f"\nSave Output ID: {save_output_id}")
    print(f"Join Inputs:    {join_inputs}")

    # 4. ASSERTION
    # The output of the SAVE must be one of the inputs of the JOIN
    assert save_output_id in join_inputs, \
        f"Broken Link! Join is not consuming the file created by Save. \nExpected {save_output_id} in {join_inputs}"