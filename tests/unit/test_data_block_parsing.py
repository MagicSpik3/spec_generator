import pytest
from src.importers.spss.parser import SpssParser
from src.importers.spss.ast import GenericNode, ComputeNode

class TestDataBlockParsing:
    
    def setup_method(self):
        self.parser = SpssParser()

    def test_ignores_inline_data_block(self):
        """
        Scenario: SPSS files often contain inline data between BEGIN DATA and END DATA.
        
        Current Behavior (Bug): 
        The parser treats '101', '202', '303' as Generic commands.
        
        Desired Behavior: 
        The parser should skip the data block entirely and resume parsing 
        at the next valid command (COMPUTE).
        """
        code = """
        BEGIN DATA
        101 202
        303 404
        END DATA.
        COMPUTE x = 1.
        """
        nodes = self.parser.parse(code)
        
        # 1. Check we have the COMPUTE node (Parsing resumed)
        # We search the list because the number of nodes might vary depending on 
        # how we fix it (skipping vs explicit DataNode)
        compute_nodes = [n for n in nodes if isinstance(n, ComputeNode)]
        assert len(compute_nodes) == 1
        assert compute_nodes[0].target == "x"

        # 2. PROOF OF BUG FIX:
        # We must NOT find a GenericNode with the value "101" or "BEGIN"
        # Currently, this assertion will FAIL because they exist in your output.
        commands = [n.command for n in nodes if isinstance(n, GenericNode)]
        
        assert "BEGIN" not in commands, "Parser incorrectly treated BEGIN DATA as a command"
        assert "101" not in commands, "Parser incorrectly treated raw data '101' as a command"
        assert "303" not in commands, "Parser incorrectly treated raw data '303' as a command"

    def test_handles_inline_data_with_terminators(self):
        """
        Edge Case: Data lines might contain dots, which shouldn't trigger
        the command terminator logic if we are inside a data block.
        """
        code = """
        BEGIN DATA
        1.5 2.5
        END DATA.
        COMPUTE y = 2.
        """
        nodes = self.parser.parse(code)
        
        # Ensure we didn't crash on the decimal points or treat '1.5' as a command
        commands = [n.command for n in nodes if isinstance(n, GenericNode)]
        assert "1.5" not in commands
        
        # Verify valid command after
        assert isinstance(nodes[-1], ComputeNode)
        assert nodes[-1].target == "y"