import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import GenericNode

class TestPendingFeatures:
    """
    These tests document 'Feature Escapes' - commands that are currently
    parsed as GenericNode but SHOULD be promoted to specific Nodes in the future.
    """
    
    def setup_method(self):
        self.parser = SpssParser()

    def test_recode_is_currently_generic(self):
        """
        RECODE is a complex transformation (Case/When).
        Currently falls back to Generic.
        """
        code = "RECODE age (Lowest thru 18 = 1) (Else = 0) INTO is_minor."
        nodes = self.parser.parse(code)
        
        # 🚩 RED FLAG: This assertion confirms we are currently failing to parse RECODE deeply
        assert isinstance(nodes[0], GenericNode)
        assert nodes[0].command == "RECODE"
        # If we implement RecodeNode later, this test will break, reminding us to update it.

    def test_string_decl_is_currently_generic(self):
        """
        STRING age_group (A10).
        Currently falls back to Generic.
        """
        code = "STRING age_group (A10)."
        nodes = self.parser.parse(code)
        
        assert isinstance(nodes[0], GenericNode)
        assert nodes[0].command == "STRING"

    def test_data_list_is_currently_generic(self):
        """
        DATA LIST FREE / id (F8.0).
        Currently falls back to Generic (likely parsing 'DATA' or 'DATA LIST').
        """
        code = "DATA LIST FREE / id (F8.0)."
        nodes = self.parser.parse(code)
        
        # Ideally this should be a Schema Definition node
        assert isinstance(nodes[0], GenericNode)
        # Depending on lexer, might catch DATA or DATA LIST. 
        # Our current lexer doesn't have a token for 'DATA LIST', so it sees IDENTIFIERS
        assert nodes[0].command in ["DATA", "DATA LIST"]