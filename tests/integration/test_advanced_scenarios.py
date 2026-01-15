import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.graph_builder import GraphBuilder
from etl_ir.types import OpType

class TestAdvancedScenarios:
    
    def test_full_etl_lifecycle(self):
        """
        Scenario: "The Kitchen Sink"
        1. LOAD raw data.
        2. COMPUTE derived columns (Date Math).
        3. FILTER rows (Clean data).
        4. MATERIALIZE (Execute).
        5. AGGREGATE (Create summary statistics).
        6. JOIN (Enrich original data with summary).
        7. SAVE (Final output).
        
        This validates that the 'active_dataset_id' is correctly handed off 
        through every stage of a complex graph.
        """
        code = """
        * 1. Load.
        GET DATA /TYPE=TXT /FILE='raw_claims.csv'.
        
        * 2. Compute.
        COMPUTE risk_score = claim_amount / age.
        
        * 3. Filter.
        SELECT IF (risk_score > 100).
        EXECUTE.
        
        * 4. Aggregate (Branching logic).
        AGGREGATE
          /OUTFILE='risk_summary.sav'
          /BREAK=region
          /avg_risk = MEAN(risk_score).
          
        * 5. Join (Merging logic).
        MATCH FILES /FILE=* /FILE='risk_summary.sav' /BY region.
        
        * 6. Save.
        SAVE OUTFILE='final_analysis.sav'.
        """
        
        # 1. Parse & Build
        parser = SpssParser()
        nodes = parser.parse(code)
        
        builder = GraphBuilder()
        pipeline = builder.build(nodes)
        
        # 2. Validate Topology
        ops = pipeline.operations
        
        # We expect exactly 7 operations based on the logic above:
        # Load -> Compute -> Filter -> Exec -> Agg -> Join -> Save
        # Note: The GraphBuilder might generate extra generic nodes if we aren't careful,
        # so we check the sequence of TYPES, which is what matters.
        
        op_types = [op.type for op in ops]
        
        expected_sequence = [
            OpType.LOAD_CSV,
            OpType.COMPUTE_COLUMNS,
            OpType.FILTER_ROWS,
            OpType.MATERIALIZE,
            OpType.AGGREGATE,
            OpType.JOIN,
            OpType.SAVE_BINARY
        ]
        
        assert op_types == expected_sequence
        
        # 3. Validate Data Lineage (The "Golden Thread")
        # The Join (Index 5) must receive data from the Aggregate (Index 4)
        agg_op = ops[4]
        join_op = ops[5]
        
        # The aggregate output should be the second input to the join (lookup table)
        # Input 0 is '*', Input 1 is 'risk_summary.sav'
        assert agg_op.outputs[0] in join_op.inputs or \
               f"source_{agg_op.parameters['outfile']}" in join_op.inputs

    def test_variable_block_parsing_in_pipeline(self):
        """
        Ensures the parser's variables block logic works inside a full pipeline context.
        """
        code = """
        GET DATA /TYPE=TXT /FILE='dates.csv'
        /VARIABLES = 
          id F8.0
          start_date DATE10.
        SAVE OUTFILE='out.sav'.
        """
        pipeline = GraphBuilder().build(SpssParser().parse(code))
        
        # Check the schema of the first dataset
        first_ds = pipeline.datasets[0]
        col_names = [c.name for c in first_ds.columns]
        
        assert "id" in col_names
        assert "start_date" in col_names