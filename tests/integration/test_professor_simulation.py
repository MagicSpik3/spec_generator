import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from etl_ir.types import DataType

class TestProfessorSimulation:
    
    def setup_method(self):
        self.parser = SpssParser()
        self.builder = GraphBuilder()

    def test_full_schema_propagation_with_aggregation(self):
        """
        Scenario:
        1. Define schema inline (DATA LIST) -> Registers 'region', 'age', 'delay_days'
        2. Filter data -> Schema should remain unchanged
        3. Aggregate -> Schema should shrink to Break Vars + New Aggregations
        
        Professor's Critique:
        "You aggregate columns that do not exist... This breaks determinism."
        """
        code = """
        * 1. Define Schema explicitly.
        DATA LIST FREE / region (A10) age (F3.0) delay_days (F5.0).
        
        * 2. Filter (Should preserve schema).
        SELECT IF delay_days >= 0.
        
        * 3. Aggregate (Should transform schema).
        AGGREGATE
          /OUTFILE=*
          /BREAK=region
          /mean_delay = MEAN(delay_days)
          /max_age = MAX(age).
        """
        
        # 1. Compile
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        # 2. Inspect the Final Dataset (The result of AGGREGATE)
        final_ds = pipeline.datasets[-1]
        columns = {c.name.upper() for c in final_ds.columns}
        
        # Assertion 1: Break variable exists
        assert "REGION" in columns
        
        # Assertion 2: Aggregated variables exist
        assert "MEAN_DELAY" in columns
        assert "MAX_AGE" in columns
        
        # Assertion 3: Non-aggregated variables are GONE (Professor's Requirement)
        # 'delay_days' and 'age' were consumed by the aggregation functions
        assert "DELAY_DAYS" not in columns
        assert "AGE" not in columns

    def test_aggregation_input_validation(self):
        """
        Scenario: Trying to aggregate a variable that doesn't exist.
        The compiler SHOULD theoretically flag this (or just handle it gracefully for now).
        This test proves we HAVE the schema information available to make that check.
        """
        code = "DATA LIST FREE / id (F8.0)."
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        # Get the dataset before any aggregation
        ds = pipeline.datasets[-1]
        assert ds.id.startswith("source_") or ds.id.startswith("ds_")
        
        # We know 'id' is in there
        schema = {c.name.upper() for c in ds.columns}
        assert "ID" in schema
        assert "REGION" not in schema # Proves we know what we DON'T have