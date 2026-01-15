import yaml
import pandas as pd
import sys
import os

def run_interpreter(yaml_path, input_csv_map, output_dir):
    print(f"🔮 Interpreter: Executing {yaml_path}...")
    
    with open(yaml_path, 'r') as f:
        pipeline = yaml.safe_load(f)

    # State Store: Holds the DataFrames in memory
    # Key = dataset_id (e.g., 'ds_001'), Value = DataFrame
    state = {}

    for op in pipeline['operations']:
        op_type = op['type']
        op_id = op['id']
        params = op.get('parameters', {})
        
        # 1. LOAD
        if op_type == 'load_csv':
            # Determine filename: Check params first, then the explicit input map
            filename = params.get('filename')
            # If the filename from YAML matches a key in our input map, use the real path
            # Otherwise assume it's a local file
            real_path = input_csv_map.get(filename, filename)
            
            print(f"  [{op_id}] Loading {real_path}...")
            df = pd.read_csv(real_path)
            
            # Register outputs
            for out_id in op['outputs']:
                state[out_id] = df.copy()

        # 2. COMPUTE (Batch or Single)
        elif op_type in ['compute_columns', 'batch_compute']:
            in_id = op['inputs'][0]
            df = state[in_id].copy()
            
            computes = []
            if op_type == 'batch_compute':
                computes = params.get('computes', [])
            else:
                computes = [{'target': params['target'], 'expression': params['expression']}]
                
            print(f"  [{op_id}] Computing {len(computes)} variables...")
            
            for comp in computes:
                target = comp['target']
                expr = comp['expression']
                # Trivial translation from SQL-like/SPSS logic to Pandas
                # Note: 'eval' handles simple math (revenue - cost) automatically
                try:
                    df[target] = df.eval(expr)
                except Exception as e:
                    print(f"    ⚠️ Error evaluating '{expr}': {e}")
            
            for out_id in op['outputs']:
                state[out_id] = df

        # 3. FILTER
        elif op_type in ['filter_rows', 'select_if']: # Handle aliases
            in_id = op['inputs'][0]
            df = state[in_id]
            condition = params.get('condition')
            
            print(f"  [{op_id}] Filtering: {condition}")
            try:
                # Pandas query syntax is very similar to SQL/SPSS
                # We might need light regex replacement here (e.g. '=' to '==')
                clean_cond = condition.replace("=", "==").replace("====", "==") 
                df = df.query(clean_cond)
            except Exception as e:
                print(f"    ⚠️ Filter failed: {e}")

            for out_id in op['outputs']:
                state[out_id] = df

        # 4. MATERIALIZE / PASS-THROUGH
        elif op_type == 'materialize':
            in_id = op['inputs'][0]
            for out_id in op['outputs']:
                state[out_id] = state[in_id] # Reference copy

        # 5. SAVE
        elif op_type == 'save_binary' or op_type == 'save_csv':
            in_id = op['inputs'][0]
            df = state[in_id]
            out_filename = params.get('filename', 'output.csv')
            out_path = os.path.join(output_dir, f"verified_{out_filename}")
            
            print(f"  [{op_id}] Saving to {out_path}...")
            df.to_csv(out_path, index=False)

        else:
            print(f"  ⚠️ Skipping unsupported op: {op_type}")

if __name__ == "__main__":
    # Usage: python interpreter.py <yaml_file> <input_csv> <output_dir>
    if len(sys.argv) < 4:
        print("Usage: python interpreter.py <yaml> <input_csv> <out_dir>")
        sys.exit(1)
        
    yaml_file = sys.argv[1]
    input_csv = sys.argv[2] # For demo, assuming single input
    out_dir = sys.argv[3]
    
    # Simple map for the demo
    # In a real app, parse the YAML to find what filename it expects
    # and map it to the input_csv argument.
    input_map = {
        "demo_data.csv": input_csv
    }
    
    run_interpreter(yaml_file, input_map, out_dir)