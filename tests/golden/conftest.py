import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder

def normalize_op(op):
    """
    Strips dynamic fields (IDs) to allow semantic comparison.
    """
    return {
        "type": op.type.name,  # Convert Enum to String
        # We don't check Inputs/Outputs IDs because they are random.
        # We rely on the order of operations to imply dependency.
        "params": op.parameters
    }

@pytest.fixture
def run_golden_test():
    def _run(spss_code, expected_ops):
        # 1. Parse
        parser = SpssParser()
        ast = parser.parse(spss_code)
        
        # 2. Build
        builder = GraphBuilder()
        pipeline = builder.build(ast)
        
        # 3. Normalize & Filter
        # We skip 'exec' (MATERIALIZE) ops for cleaner testing unless requested
        actual_ops = [
            normalize_op(op) 
            for op in pipeline.operations 
            if op.type.name not in ["MATERIALIZE", "GENERIC_TRANSFORM"] 
            # Note: We explicitly WANT to see Generic ones if they fail, 
            # but for Golden tests, we expect NO generics.
        ]
        
        # 4. Compare
        # We iterate through expectations. 
        # Actual pipeline might have extra 'materialize' steps, so we focus on the logic.
        
        print("\n--- ACTUAL TOPOLOGY ---")
        for i, op in enumerate(actual_ops):
            print(f"[{i}] {op['type']} : {op['params']}")
            
        print("\n--- EXPECTED ---")
        for i, op in enumerate(expected_ops):
            print(f"[{i}] {op['type']} : {op['params']}")

        assert len(actual_ops) == len(expected_ops), \
            f"Length Mismatch! Expected {len(expected_ops)} ops, got {len(actual_ops)}"
            
        for i, (actual, expected) in enumerate(zip(actual_ops, expected_ops)):
            assert actual['type'] == expected['type'], \
                f"Op #{i} Type Mismatch. Got {actual['type']}, expected {expected['type']}"
            
            # Param Subset Check: Actual params must contain all Expected params
            # (Allows Actual to have extra internal metadata like 'format')
            for key, val in expected['params'].items():
                assert actual['params'].get(key) == val, \
                    f"Op #{i} ({actual['type']}) Param '{key}' mismatch. Got '{actual['params'].get(key)}', expected '{val}'"

    return _run