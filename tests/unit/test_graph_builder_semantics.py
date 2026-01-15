import pytest
from src.importers.spss.graph_builder import GraphBuilder
from src.importers.spss.ast import FilterNode, MaterializeNode, JoinNode, LoadNode
from etl_ir.types import OpType

class TestGraphBuilderSemantics:
    
    def setup_method(self):
        self.builder = GraphBuilder()

    def test_builds_filter_operation(self):
        """
        Scenario: A FilterNode should create a new dataset state and a FILTER op.
        """
        # 1. Setup State (Load Data)
        load_node = LoadNode(filename="data.csv")
        filter_node = FilterNode(condition="age > 18")
        
        # 2. Build
        pipeline = self.builder.build([load_node, filter_node])
        
        # 3. Verify
        ops = pipeline.operations
        assert len(ops) == 2
        assert ops[1].type == OpType.FILTER_ROWS
        assert ops[1].parameters['condition'] == "age > 18"
        
        # Verify Lineage
        assert ops[1].inputs == ops[0].outputs

    def test_builds_materialize_barrier(self):
        """
        Scenario: EXECUTE should create a MATERIALIZE barrier.
        """
        load_node = LoadNode(filename="data.csv")
        exec_node = MaterializeNode()
        
        pipeline = self.builder.build([load_node, exec_node])
        
        ops = pipeline.operations
        assert ops[1].type == OpType.MATERIALIZE
        # Materialize ensures the previous step is computed/written to disk in SPSS context
        assert ops[1].inputs == ops[0].outputs

    def test_builds_join_topology(self):
        """
        Scenario: MATCH FILES should create a JOIN op with multiple inputs.
        """
        # 1. Load Main Data
        load_main = LoadNode(filename="main.sav")
        
        # 2. Join with lookup (Active Dataset '*' + 'rates.sav')
        join_node = JoinNode(
            sources=["*", "rates.sav"],
            by=["id"]
        )
        
        pipeline = self.builder.build([load_main, join_node])
        
        ops = pipeline.operations
        join_op = ops[1]
        
        assert join_op.type == OpType.JOIN
        assert join_op.parameters['by'] == "id"
        
        # Critical: Verify Inputs
        # Input 0: The output of load_main (because of '*')
        # Input 1: An implicitly created source for 'rates.sav'
        assert len(join_op.inputs) == 2
        assert join_op.inputs[0] == ops[0].outputs[0] 
        assert "source_rates.sav" in join_op.inputs[1]
        
        # Verify the implicit dataset was registered
        ds_ids = [ds.id for ds in pipeline.datasets]
        assert "source_rates.sav" in ds_ids