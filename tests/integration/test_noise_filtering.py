import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
# 🟢 FIX: Import the Enum so we can match correctly
from etl_ir.types import OpType

def test_data_flow_survives_noise():
    """
    Scenario: Load -> Noise -> Compute -> Noise -> Save
    Goal: Ensure the 'Save' operation receives the dataset created by 'Compute',
          skipping the 'Noise' in between.
    """
    # 1. Setup Script
    script = """
    DATA LIST FREE / id.
    TITLE "Step 1: Loading".
    COMPUTE x = id * 2.
    FREQUENCIES VARIABLES=x.
    SAVE OUTFILE='final.sav'.
    """

    # 2. Parse & Build
    parser = SpssParser()
    ast = parser.parse(script)
    
    builder = GraphBuilder()
    pipeline = builder.build(ast)

    # 3. Analyze the Chain
    ops = pipeline.operations
    
    # 🟢 DEBUG: Print what we actually got to be sure
    print("\nOperations Found:")
    for op in ops:
        print(f" - {op.type} (ID: {op.id})")

    # 🟢 FIX: Compare against the Enum, not a string
    compute_op = next((op for op in ops if op.type == OpType.COMPUTE_COLUMNS), None)
    save_op = next((op for op in ops if op.type == OpType.SAVE_BINARY), None)
    
    assert compute_op, "Compute op missing (Check OpType match)"
    assert save_op, "Save op missing (Check OpType match)"

    # 4. CRITICAL ASSERTION: The Chain Link
    compute_output_id = compute_op.outputs[0]
    save_input_id = save_op.inputs[0]

    print(f"\n🔗 Chain Check:")
    print(f"   Compute Output: {compute_output_id}")
    print(f"   [NOISE HAPPENED HERE]")
    print(f"   Save Input:     {save_input_id}")

    assert save_input_id == compute_output_id, \
        f"💔 Chain Broken! Save is reading '{save_input_id}', but Compute produced '{compute_output_id}'"

    # 5. Check for 'Generic' pollution
    generic_ops = [op for op in ops if op.type == OpType.GENERIC_TRANSFORM]
    assert len(generic_ops) == 0, f"Found {len(generic_ops)} Generic Ops! Noise was not filtered."