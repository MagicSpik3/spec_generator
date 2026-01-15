import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.ast import GenericNode, LoadNode, JoinNode, ComputeNode
from etl_ir.types import DataType
from etl_ir.model import Column

class TestParserEdgeCases:
    
    def setup_method(self):
        self.parser = SpssParser()

    # --------------------------------------------------------------------------
    # 1. Generic Command Fallback (Lines 38-41, 115-121)
    # --------------------------------------------------------------------------
    def test_parses_unknown_command_as_generic(self):
        """
        Scenario: Parser encounters 'FREQUENCIES'. It should consume it 
        as a GenericNode rather than crashing.
        """
        code = "FREQUENCIES VARIABLES=age."
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 1
        assert isinstance(nodes[0], GenericNode)
        assert nodes[0].command == "FREQUENCIES"

    # --------------------------------------------------------------------------
    # 2. Variables Block Parsing (Line 59)
    # --------------------------------------------------------------------------
    def test_parses_get_data_variables(self):
        """
        Scenario: GET DATA with a /VARIABLES block.
        We need to verify the column schema is actually parsed.
        """
        code = """
        GET DATA /TYPE=TXT /FILE='data.csv'
        /VARIABLES = id F8.0 name A20 date_col DATE10.
        """
        nodes = self.parser.parse(code)
        
        assert isinstance(nodes[0], LoadNode)
        cols = nodes[0].columns
        
        # Verify the parsing logic inside _parse_variables_block
        assert len(cols) == 3
        assert cols[0] == Column(name="id", type=DataType.INTEGER)
        assert cols[1] == Column(name="name", type=DataType.STRING)
        assert cols[2] == Column(name="date_col", type=DataType.DATE)

    # --------------------------------------------------------------------------
    # 3. Compute Syntax Errors (Lines 88, 94)
    # --------------------------------------------------------------------------
    def test_compute_raises_error_on_missing_target(self):
        """
        Scenario: 'COMPUTE = 1.' (Missing the variable name).
        """
        code = "COMPUTE = 1."
        with pytest.raises(SyntaxError, match="Expected Identifier"):
            self.parser.parse(code)

    def test_compute_raises_error_on_missing_equals(self):
        """
        Scenario: 'COMPUTE x 1.' (Missing the = sign).
        """
        code = "COMPUTE x 1."
        with pytest.raises(SyntaxError, match="Expected '='"):
            self.parser.parse(code)

    # --------------------------------------------------------------------------
    # 4. Ignored Subcommands in Match Files (Lines 226, 234-236)
    # --------------------------------------------------------------------------
    def test_match_files_ignores_extra_subcommands(self):
        """
        Scenario: MATCH FILES often has /IN=source_flag or /MAP.
        We should parse the join correctly and ignore the noise.
        """
        code = "MATCH FILES /FILE=* /IN=flag /FILE='lookup.sav' /MAP /BY id."
        nodes = self.parser.parse(code)
        
        assert isinstance(nodes[0], JoinNode)
        # Should still capture both files despite the /IN and /MAP noise
        assert nodes[0].sources == ["*", "lookup.sav"]
        assert nodes[0].by == ["id"]