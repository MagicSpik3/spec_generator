import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import DataListNode
from src.ir.types import DataType

class TestDataListParsing:
    def setup_method(self):
        self.parser = SpssParser()

    def test_parses_data_list_free(self):
        """
        Scenario: DATA LIST defines the schema for inline data.
        We need to extract these column definitions.
        """
        code = "DATA LIST FREE / id (F8.0) region (A10) age (F3.0)."
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 1
        assert isinstance(nodes[0], DataListNode)
        
        # Verify schema extraction
        # We expect a list of (name, type) tuples
        cols = nodes[0].columns
        assert len(cols) == 3
        assert cols[0] == ("id", DataType.INTEGER)
        assert cols[1] == ("region", DataType.STRING)
        assert cols[2] == ("age", DataType.INTEGER)