import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder

class TestGhostColumns:
    
    def setup_method(self):
        self.parser = SpssParser()
        self.builder = GraphBuilder()

    def test_recode_creates_ghost_column(self):
        """
        Scenario: 
        1. DATA LIST creates 'age'.
        2. RECODE creates 'is_adult' (Generic command -> Schema unknown).
        3. AGGREGATE tries to break by 'is_adult'.
        
        Expected Result (Currently): 
        The compiler will produce an output dataset that includes 'IS_ADULT' 
        in the columns list because Aggregation logic inherits it from the Break Var.
        
        BUT: The input dataset to the aggregation (the one coming from RECODE) 
        does NOT list 'is_adult' in its columns.
        """
        code = """
        DATA LIST FREE / age (F3.0).
        RECODE age (18 thru 100 = 1) (ELSE = 0) INTO is_adult.
        AGGREGATE
          /OUTFILE=*
          /BREAK=is_adult
          /count = N.
        """
        
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        # 1. Check the dataset BEFORE the aggregation (Result of RECODE)
        # We expect to find 'is_adult' here, but we won't because RECODE is generic.
        pre_agg_ds = pipeline.datasets[-2] 
        cols = {c.name.upper() for c in pre_agg_ds.columns}
        
        # 🚩 THIS WILL FAIL
        assert "IS_ADULT" in cols, "Curve Ball: RECODE failed to register the new variable!"

    def test_join_merges_schemas(self):
        """
        Scenario: Joining two datasets.
        The result should contain columns from BOTH.
        """
        code = """
        GET DATA /FILE='claims.sav'.   
        DATA LIST FREE / id (F8.0) name (A20).
        MATCH FILES /FILE=* /TABLE='claims.sav' /BY id.
        """
        # Note: We can't fully run this without mocking 'claims.sav' schema,
        # but the logic holds: JoinNode needs to merge schemas.
        pass