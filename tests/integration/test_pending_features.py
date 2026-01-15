import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.ast import GenericNode, DataListNode, RecodeNode

class TestPendingFeatures:
    """
    These tests document features moving from 'Generic' (Debt) to 'Supported' (Done).
    """
    
    def setup_method(self):
        self.parser = SpssParser()

    def test_recode_is_NOW_supported(self):
        """
        🟢 GRADUATED: RECODE is no longer Generic!
        It now parses into a typed RecodeNode.
        """
        code = "RECODE age (Lowest thru 18 = 1) (Else = 0) INTO is_minor."
        nodes = self.parser.parse(code)
        
        # Assert it is the specific node
        assert isinstance(nodes[0], RecodeNode)
        
        # Verify we captured the schema flow
        assert nodes[0].source_vars == ["age"]
        assert nodes[0].target_vars == ["is_minor"]
        # We don't check the complex map_logic string deeply, just that we have the node.

    def test_string_decl_is_currently_generic(self):
        """
        STRING age_group (A10). 
        Still Generic (Next on the list?).
        """
        code = "STRING age_group (A10)."
        nodes = self.parser.parse(code)
        assert isinstance(nodes[0], GenericNode)

    def test_data_list_is_NOW_supported(self):
        """
        🟢 GRADUATED: DATA LIST is no longer Generic!
        """
        code = "DATA LIST FREE / id (F8.0)."
        nodes = self.parser.parse(code)
        
        assert isinstance(nodes[0], DataListNode)
        assert len(nodes[0].columns) == 1
        assert nodes[0].columns[0].name == "id"