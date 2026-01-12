import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import ComputeNode, FilterNode, MaterializeNode

class TestSemanticParsing:
    """
    Validates that the parser correctly separates distinct logical operations
    that often appear on the same line in legacy SPSS.
    """
    
    def setup_method(self):
        self.parser = SpssParser()

    def test_separates_filter_from_compute(self):
        """
        Critical Fix: 'COMPUTE x=1. SELECT IF (x>0).' should be TWO nodes,
        not one giant Compute node with a messy expression.
        """
        code = "COMPUTE x=1. SELECT IF (x>0)."
        nodes = self.parser.parse(code)
        
        # Current (Broken) Behavior: 1 Node (Compute)
        # Desired Behavior: 2 Nodes (Compute, Filter)
        assert len(nodes) == 2
        assert isinstance(nodes[0], ComputeNode)
        assert isinstance(nodes[1], FilterNode)
        
        # 🟢 FIX: Normalize whitespace for comparison
        # This makes the test robust against token spacing changes
        assert nodes[1].condition.replace(" ", "") == "(x>0)"

    def test_separates_execute_barrier(self):
        """
        EXECUTE is a barrier that forces materialization. 
        It must be its own node.
        """
        code = "COMPUTE y=2. EXECUTE."
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 2
        assert isinstance(nodes[1], MaterializeNode)