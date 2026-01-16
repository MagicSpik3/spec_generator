import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from spec_generator.importers.spss.ast import JoinNode, SaveNode

class TestTopologyDeepDive:

    # =========================================================================
    # LEVEL 1: The Parser (Is the input valid?)
    # =========================================================================
    def test_01_parser_extraction(self):
        """
        Does the Parser correctly extract the filename from the MATCH FILES command?
        If this fails, the Builder never stood a chance.
        """
        print("\n🔎 LEVEL 1: PARSER DIAGNOSTICS")
        script = "MATCH FILES /FILE=* /TABLE='temp.sav' /BY id."
        
        parser = SpssParser()
        ast = parser.parse(script)
        
        # We expect a JoinNode
        join_node = next((n for n in ast if isinstance(n, JoinNode)), None)
        assert join_node, "Parser failed to create a JoinNode"
        
        print(f"  AST Node: {join_node}")
        print(f"  Sources found: {join_node.sources}")
        
        # CRITICAL CHECK: Does it see 'temp.sav'?
        # We check loosely to allow for quoted or unquoted variations
        has_temp = any("temp.sav" in s for s in join_node.sources)
        assert has_temp, f"Parser did not extract 'temp.sav'. Got: {join_node.sources}"

    # =========================================================================
    # LEVEL 2: The Builder State (Is the anchor dropping?)
    # =========================================================================
    def test_02_builder_save_registration(self):
        """
        Does the Builder correctly register the SAVED file in its internal
        dataset registry?
        """
        print("\n🔎 LEVEL 2: BUILDER STATE DIAGNOSTICS")
        script = "DATA LIST FREE / id. SAVE OUTFILE='temp.sav'."
        
        parser = SpssParser()
        ast = parser.parse(script)
        
        builder = GraphBuilder()
        builder.build(ast)
        
        print(f"  Registered Datasets: {[d.id for d in builder.datasets]}")
        
        # We expect an internal dataset ID starting with 'file_'
        # This confirms we are "advertising" the file availability.
        match = next((d for d in builder.datasets if "temp.sav" in d.id), None)
        assert match, "Builder ignored the SAVE command; no 'file_temp.sav' registered."
        print(f"  ✅ Found registered dataset: {match.id}")

    # =========================================================================
    # LEVEL 3: The Integration (The Handshake)
    # =========================================================================
    @pytest.mark.parametrize("save_fn, join_fn", [
        ("'temp.sav'", "'temp.sav'"), # Quoted
        ("temp.sav", "temp.sav"),     # Unquoted
    ])
    def test_03_the_handshake(self, save_fn, join_fn):
        """
        Now we test the logic that connects Level 1 and Level 2.
        """
        print(f"\n🔎 LEVEL 3: HANDSHAKE ({save_fn} -> {join_fn})")
        
        script = f"""
        DATA LIST FREE / id. 
        SAVE OUTFILE={save_fn}.
        MATCH FILES /FILE=* /TABLE={join_fn} /BY id.
        """
        
        parser = SpssParser()
        ast = parser.parse(script)
        
        builder = GraphBuilder()
        pipeline = builder.build(ast)
        
        join_op = next(op for op in pipeline.operations if op.type == "join")
        
        print(f"  Builder Datasets: {[d.id for d in builder.datasets]}")
        print(f"  Join Inputs: {join_op.inputs}")
        
        # If this fails, we know exactly *what* ID was registered 
        # vs *what* ID was requested.
        assert len(join_op.inputs) == 2, "Join failed to pick up the second file."