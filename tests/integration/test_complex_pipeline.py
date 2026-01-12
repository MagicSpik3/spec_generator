import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.graph_builder import GraphBuilder
from src.ir.types import OpType

class TestComplexPipeline:
    def test_aggregate_and_join_flow(self):
        """
        Scenario: 
        1. Load Claims
        2. Aggregate to get Total Claim Amount per Person
        3. Join that Total back to the main table (Lookup)
        
        This mimics a very common ETL pattern: "Window Function via Join".
        """
        code = """
        GET DATA /TYPE=TXT /FILE='claims.csv'.
        
        * Step 1: Create Summary.
        AGGREGATE
          /OUTFILE='agg_claims.sav'
          /BREAK=person_id
          /total_amt = SUM(claim_amount).
          
        * Step 2: Join back.
        MATCH FILES /FILE=* /FILE='agg_claims.sav' /BY person_id.
        """
        
        # 1. Parse
        parser = SpssParser()
        nodes = parser.parse(code)
        
        # 2. Build Graph
        builder = GraphBuilder()
        pipeline = builder.build(nodes)
        
        # 3. Validation
        ops = pipeline.operations
        
        # Check Operation Types
        assert ops[0].type == OpType.LOAD
        assert ops[1].type == OpType.AGGREGATE # This will fail until we implement the Builder logic
        assert ops[2].type == OpType.JOIN
        
        # Check Topology/Lineage
        # The Join (op 2) should have inputs from:
        # - The original loaded data (or its derived state)
        # - The OUTPUT of the aggregate operation
        
        agg_op = ops[1]
        join_op = ops[2]
        
        # The aggregate output ID (e.g., file_agg_claims.sav) should be in the Join inputs
        agg_output_id = agg_op.outputs[0]
        assert agg_output_id in join_op.inputs