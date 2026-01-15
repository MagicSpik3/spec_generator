import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.graph_builder import GraphBuilder
from etl_ir.types import DataType

class TestSchemaPropagation:
    
    def setup_method(self):
        self.parser = SpssParser()
        self.builder = GraphBuilder()

    def test_data_list_injects_schema(self):
        """
        Scenario: DATA LIST defines new columns (e.g., 'age').
        The resulting Dataset in the IR must have these columns.
        """
        code = "DATA LIST FREE / id (F8.0) age (F3.0)."
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        # Get the resulting dataset
        ds = pipeline.datasets[-1]
        
        # Check columns exist (Case Insensitive)
        col_names = [c.name.upper() for c in ds.columns]
        assert "ID" in col_names
        assert "AGE" in col_names
        
        # Check types (We need to find the specific column to check type)
        age_col = next(c for c in ds.columns if c.name.upper() == "AGE")
        assert age_col.type == DataType.INTEGER

    def test_aggregate_calculates_output_schema(self):
        """
        Scenario: AGGREGATE changes the schema entirely.
        Input: [region, sales]
        Output: [region, total_sales]
        """
        code = """
        DATA LIST FREE / region (A10) sales (F8.0).
        AGGREGATE
          /OUTFILE=*
          /BREAK=region
          /total_sales = SUM(sales).
        """
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        # The last dataset is the result of the AGGREGATE
        agg_ds = pipeline.datasets[-1]
        cols = [c.name.upper() for c in agg_ds.columns]

        # 1. Break variable must be preserved
        assert "REGION" in cols
        
        # 2. Aggregation target must be created
        assert "TOTAL_SALES" in cols
        
        # 3. 'sales' should NOT be there (unless it's in the break)
        assert "SALES" not in cols