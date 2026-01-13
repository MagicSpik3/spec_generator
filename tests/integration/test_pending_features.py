import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import GenericNode, DataListNode

class TestPendingFeatures:
    """
    These tests document 'Feature Escapes' - commands that are currently
    parsed as GenericNode but SHOULD be promoted to specific Nodes in the future.
    """
    
    def setup_method(self):
        self.parser = SpssParser()

    def test_recode_is_currently_generic(self):
        """
        RECODE is a complex transformation. Currently Generic.
        """
        code = "RECODE age (Lowest thru 18 = 1) (Else = 0) INTO is_minor."
        nodes = self.parser.parse(code)
        assert isinstance(nodes[0], GenericNode)

    def test_string_decl_is_currently_generic(self):
        """
        STRING age_group (A10). Currently Generic.
        """
        code = "STRING age_group (A10)."
        nodes = self.parser.parse(code)
        assert isinstance(nodes[0], GenericNode)

    def test_data_list_is_NOW_supported(self):
        """
        🟢 GRADUATED: DATA LIST is no longer Generic!
        It now parses into a typed DataListNode.
        """
        code = "DATA LIST FREE / id (F8.0)."
        nodes = self.parser.parse(code)
        
        # We now assert it IS a DataListNode
        assert isinstance(nodes[0], DataListNode)
        assert len(nodes[0].columns) == 1
        assert nodes[0].columns[0][0] == "id"