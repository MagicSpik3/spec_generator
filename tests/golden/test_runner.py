import pytest
import yaml
import os
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder

# 1. Load the Test Cases
YAML_PATH = os.path.join(os.path.dirname(__file__), "cases.yaml")

def load_cases():
    if not os.path.exists(YAML_PATH):
        return []
    with open(YAML_PATH, "r") as f:
        return yaml.safe_load(f)

# 2. Define the Normalizer (Strip random IDs)
def normalize_quotes(val):
    """
    Standardizes strings by stripping surrounding quotes.
    '"LOW"' -> 'LOW'
    "'LOW'" -> 'LOW'
    """
    s = str(val).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def normalize_pipeline(pipeline):
    clean_ops = []
    for op in pipeline.operations:
        # 🟢 FIX: Only skip MATERIALIZE. Show Generics so we can verify them!
        if op.type.name in ["MATERIALIZE"]: 
            continue
            
        clean_ops.append({
            "type": op.type.name,
            "params": op.parameters
        })
    return clean_ops

# 3. The Data-Driven Test Function
@pytest.mark.parametrize("case", load_cases(), ids=lambda c: c["name"])
def test_golden_scenario(case):
    print(f"\n🚀 Running Case: {case['name']}")
    print(f"📝 Description: {case['description']}")
    
    # A. Parse
    parser = SpssParser()
    try:
        ast = parser.parse(case["spss"])
    except Exception as e:
        pytest.fail(f"❌ Parser Crashed: {str(e)}")

    # B. Build
    builder = GraphBuilder()
    try:
        pipeline = builder.build(ast)
    except Exception as e:
        pytest.fail(f"❌ Builder Crashed: {str(e)}")

    # 🔍 VERBOSE: RAW STATE INSPECTOR
    # This shows exactly how operations are wired together (IDs included)
    print("\n" + "="*40)
    print("       🔍 RAW STATE (IR DUMP)")
    print("="*40)
    for op in pipeline.operations:
        # Skip housekeeping ops if you want to reduce noise, or keep them for full transparency
        if op.type.name in ["MATERIALIZE", "GENERIC_TRANSFORM"]:
             print(f"\033[90m[{op.id}] {op.type.name} (Skipped in Norm)\033[0m")
             continue
             
        print(f"[{op.id}] \033[94m{op.type.name}\033[0m")
        print(f"  ├─ Inputs:  {op.inputs}")
        print(f"  ├─ Outputs: {op.outputs}")
        print(f"  └─ Params:  {op.parameters}")
        print("-" * 20)
    print("="*40 + "\n")

    # C. Normalize
    actual_ops = normalize_pipeline(pipeline)
    expected_ops = case["expected"]

    # D. Comparison Output
    print("--- COMPARISON (Normalized) ---")
    for i, op in enumerate(actual_ops):
        status = "✅" if i < len(expected_ops) and op == expected_ops[i] else "❓"
        print(f"{status} Step {i+1}: {op['type']} {op['params']}")

    # E. Assertions
        assert len(actual_ops) == len(expected_ops), \
             f"Length Mismatch! Expected {len(expected_ops)} ops, got {len(actual_ops)}"
    
        for i, (actual, expected) in enumerate(zip(actual_ops, expected_ops)):
            assert actual["type"] == expected["type"], \
                 f"Op #{i} Type Mismatch. Got {actual['type']}, expected {expected['type']}"
            
            for key, val in expected["params"].items():
                # 🟢 FIX: Use normalize_quotes on both sides
                actual_val = normalize_quotes(actual["params"].get(key))
                expected_val = normalize_quotes(val)
                
                assert actual_val == expected_val, \
                     f"Op #{i} Param '{key}' mismatch.\n   Got:      {actual_val}\n   Expected: {expected_val}"

