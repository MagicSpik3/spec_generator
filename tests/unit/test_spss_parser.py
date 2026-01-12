import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import LoadNode, ComputeNode, SaveNode

class TestSpssParser:
    def setup_method(self):
        self.parser = SpssParser()

    def test_parses_to_ast_nodes(self):
        """
        Scenario: Parser should return a flat list of AST nodes.
        """
        code = """
        GET DATA /TYPE=TXT /FILE='raw_data.csv'.
        COMPUTE x = 1.
        SAVE OUTFILE='final.sav'.
        """
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 3
        
        # Check Load
        assert isinstance(nodes[0], LoadNode)
        assert nodes[0].filename == "raw_data.csv"
        
        # Check Compute (Updated for Split Logic)
        assert isinstance(nodes[1], ComputeNode)
        assert nodes[1].target == "x"
        assert nodes[1].expression == "1"
        
        # Check Save
        assert isinstance(nodes[2], SaveNode)
        assert nodes[2].filename == "final.sav"