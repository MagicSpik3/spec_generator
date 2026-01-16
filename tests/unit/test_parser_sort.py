import pytest
from spec_generator.importers.spss.parser import SpssParser
# 🟢 This import will fail initially, confirming the feature is missing!
from spec_generator.importers.spss.ast import SortNode, GenericNode

class TestParserSort:
    def setup_method(self):
        self.parser = SpssParser()

    def test_parse_simple_sort(self):
        """
        Scenario: Standard single-variable sort.
        """
        script = "SORT CASES BY region_code."
        ast = self.parser.parse(script)

        assert len(ast) == 1
        # It should NOT be generic
        assert not isinstance(ast[0], GenericNode), "SORT was parsed as GenericNode!"
        
        # It SHOULD be a SortNode
        assert isinstance(ast[0], SortNode)
        assert ast[0].keys == ["region_code"]

    def test_parse_multi_key_sort(self):
        """
        Scenario: Sorting by multiple columns.
        """
        script = "SORT CASES BY region_code income."
        ast = self.parser.parse(script)

        assert isinstance(ast[0], SortNode)
        assert ast[0].keys == ["region_code", "income"]

    def test_parse_sort_without_optional_keywords(self):
        """
        Scenario: SPSS allows omitting 'CASES' and 'BY' sometimes.
        """
        # Minimalist syntax
        script = "SORT VARIABLES id." 
        # Note: If your parser assumes 'CASES' is mandatory, this documents that limitation.
        # For now, let's test the standard full command.
        
        script_standard = "SORT CASES region_code." # 'BY' is often optional
        ast = self.parser.parse(script_standard)
        
        assert isinstance(ast[0], SortNode)
        assert ast[0].keys == ["region_code"]