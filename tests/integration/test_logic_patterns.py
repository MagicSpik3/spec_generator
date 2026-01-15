import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from etl_ir.types import DataType, OpType
from etl_ir.model import Column          

class TestLogicPatterns:
    
    def setup_method(self):
        self.parser = SpssParser()
        self.builder = GraphBuilder()

    def test_pattern_accumulator(self):
        """
        Pattern 1: The Accumulator
        Logic: b depends on a, c depends on b.
        Goal: Verify schema accumulates (a, b, c) and types are preserved.
        """
        code = """
        DATA LIST FREE / id (F8.0).
        COMPUTE a = 1.
        COMPUTE b = a + 1.
        COMPUTE c = b * 2.
        """
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        final_ds = pipeline.datasets[-1]
        cols = {c.name for c in final_ds.columns}
        
        # The final dataset must have ALL history, not just the last compute
        assert "id" in cols
        assert "a" in cols
        assert "b" in cols
        assert "c" in cols
        
        # Verify Lineage depth (Optimization check)
        # There should be 3 compute operations
        computes = [op for op in pipeline.operations if op.type == OpType.COMPUTE_COLUMNS]
        assert len(computes) == 3

    def test_pattern_gatekeeper(self):
        """
        Pattern 2: The Gatekeeper
        Logic: Compute X -> Filter -> Compute Y
        Goal: Verify Y is calculated ONLY on filtered data (lineage check).
        """
        code = """
        DATA LIST FREE / id (F8.0).
        COMPUTE x = 1.
        SELECT IF x > 0.
        COMPUTE y = 2.
        """
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        ops = pipeline.operations
        
        # Topology Check:
        # Load(0) -> Compute X(1) -> Filter(2) -> Compute Y(3)
        assert ops[1].type == OpType.COMPUTE_COLUMNS and ops[1].parameters['target'] == 'x'
        assert ops[2].type == OpType.FILTER_ROWS
        assert ops[3].type == OpType.COMPUTE_COLUMNS and ops[3].parameters['target'] == 'y'
        
        # Crucial: Op 3's Input must be Op 2's Output
        filter_out = ops[2].outputs[0]
        compute_y_in = ops[3].inputs[0]
        assert filter_out == compute_y_in

    def test_pattern_fork(self):
        """
        Pattern 3: The Fork (Intermediate Save)
        Logic: Compute A -> Save -> Compute B -> Save
        Goal: Verify the pipeline doesn't stop after the first save.
        """
        code = """
        DATA LIST FREE / id (F8.0).
        COMPUTE a = 1.
        SAVE OUTFILE='intermediate.sav'.
        COMPUTE b = 2.
        SAVE OUTFILE='final.sav'.
        """
        nodes = self.parser.parse(code)
        pipeline = self.builder.build(nodes)
        
        saves = [op for op in pipeline.operations if op.type == OpType.SAVE_BINARY]
        assert len(saves) == 2
        
        # Check inputs for the second save
        # It should contain 'b'
        final_save_op = saves[1]
        # We need to find the dataset it saves
        final_ds_id = final_save_op.inputs[0] # Save reads from this DS
        final_ds = next(d for d in pipeline.datasets if d.id == final_ds_id)
        
        cols = {c.name for c in final_ds.columns}
        assert "a" in cols # Inherited
        assert "b" in cols # New