import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import AggregateNode

class TestAggregateParsing:
    
    def setup_method(self):
        self.parser = SpssParser()

    def normalize(self, text: str) -> str:
        """
        Helper to make assertions case and whitespace insensitive.
        Example: "mean_age = MEAN ( age )" -> "MEAN_AGE=MEAN(AGE)"
        """
        return text.upper().replace(" ", "")

    def test_parses_aggregate_to_active_dataset(self):
        """
        Scenario: Aggregating into the current dataset (*).
        Command: AGGREGATE /OUTFILE=* /BREAK=region /mean_age = MEAN(age).
        """
        code = "AGGREGATE /OUTFILE=* /BREAK=region /mean_age = MEAN(age)."
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 1
        assert isinstance(nodes[0], AggregateNode)
        
        # Check Structure
        assert nodes[0].outfile == "*"
        assert self.normalize(nodes[0].break_vars[0]) == "REGION"
        
        # Robust Logic Check
        actual_agg = nodes[0].aggregations[0]
        assert self.normalize(actual_agg) == "MEAN_AGE=MEAN(AGE)"

    def test_parses_complex_aggregate(self):
        """
        Scenario: Aggregating to external file with multiple break vars and functions.
        """
        code = """
        AGGREGATE
          /OUTFILE='summary.sav'
          /BREAK=region date
          /total_sales = SUM(sales)
          /count_rows = N.
        """
        nodes = self.parser.parse(code)
        
        assert isinstance(nodes[0], AggregateNode)
        assert nodes[0].outfile == "summary.sav"
        
        # Check Break Vars
        break_vars = [self.normalize(v) for v in nodes[0].break_vars]
        assert break_vars == ["REGION", "DATE"]
        
        # Check Aggregations (Order matters in AST, usually)
        normalized_aggs = [self.normalize(a) for a in nodes[0].aggregations]
        
        assert "TOTAL_SALES=SUM(SALES)" in normalized_aggs
        assert "COUNT_ROWS=N" in normalized_aggs