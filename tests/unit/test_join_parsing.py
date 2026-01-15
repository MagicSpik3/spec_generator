import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.ast import JoinNode

class TestJoinParsing:
    def setup_method(self):
        self.parser = SpssParser()

    def test_parses_match_files_merge(self):
        """
        Scenario: Merging the active dataset (*) with a lookup table.
        SPSS: MATCH FILES /FILE=* /FILE='rates.sav' /BY benefit_type.
        """
        code = "MATCH FILES /FILE=* /FILE='rates.sav' /BY benefit_type."
        nodes = self.parser.parse(code)
        
        assert len(nodes) == 1
        assert isinstance(nodes[0], JoinNode)
        
        # Sources should list all files involved
        # '*' represents the current active dataset
        assert nodes[0].sources == ["*", "rates.sav"]
        
        # Join keys
        assert nodes[0].by == ["benefit_type"]

    def test_parses_multiple_join_keys(self):
        """
        Scenario: Joining on composite keys.
        """
        code = "MATCH FILES /FILE=* /FILE='other.sav' /BY region date."
        nodes = self.parser.parse(code)
        
        assert nodes[0].by == ["region", "date"]