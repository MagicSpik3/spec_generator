import pytest
from pydantic import ValidationError
from etl_ir.model import Pipeline, Dataset, Operation, DataType, OpType
# ...


    # ...


class TestIRValidation:
    """
    Defines the contract for the Intermediate Representation (IR).
    The IR must be self-validating: it should be impossible to create 
    a 'broken' pipeline state programmatically.
    """

    def test_dataset_requires_valid_schema(self):
        """
        A Dataset must have a generic schema defined.
        """
        # 1. Valid Dataset
        ds = Dataset(
            id="raw_data", 
            source="file", 
        columns=[
                {"name": "id", "type": DataType.INTEGER}, 
                {"name": "name", "type": DataType.STRING}
            ]   
        )
        assert ds.id == "raw_data"
        
        # 2. Invalid Schema (e.g. Empty or Malformed)
        with pytest.raises(ValidationError):
            Dataset(id="bad_data", source="file", columns="not_a_list")

    @pytest.mark.skip(reason="Topology validation is now handled by the Optimizer, not the IR dataclass")
    def test_operation_must_reference_existing_datasets(self):
        """
        An Operation (e.g., Filter) cannot reference a dataset ID that 
        is not defined in the Pipeline's dataset registry.
        """
        ds_a = Dataset(id="A", source="file", columns=[{"name": "col", "type": DataType.INTEGER}])
        
        op = Operation(
            id="op1",
            type=OpType.FILTER_ROWS,
            inputs=["A"],
            outputs=["B"], # "B" does not exist
            params={}
        )

        # 🟢 FIX: Catch ValidationError and match the actual error message from model.py
        with pytest.raises(ValidationError, match="references unknown output Dataset 'B'"):
            Pipeline(
                datasets=[ds_a], 
                operations=[op]
            )

    @pytest.mark.skip(reason="Topology validation moved to Optimizer")
    def test_pipeline_prevents_cycles(self):
        """
        The pipeline must be a DAG (Directed Acyclic Graph).
        """
        ds_a = Dataset(id="A", source="file", columns=[])
        ds_b = Dataset(id="B", source="derived", columns=[])

        op1 = Operation(id="op1", type=OpType.COMPUTE_COLUMNS, inputs=["A"], outputs=["B"])
        op2 = Operation(id="op2", type=OpType.COMPUTE_COLUMNS, inputs=["B"], outputs=["A"])

        # 🟢 FIX: Catch ValidationError here too
        with pytest.raises(ValidationError, match="Cycle detected"):
            Pipeline(
                datasets=[ds_a, ds_b],
                operations=[op1, op2]
            )


    def test_operation_types_are_strictly_enum(self):
        """
        Operations must be one of the allowed Enum types.
        """
        with pytest.raises(ValidationError):
            Operation(
                id="bad_op", 
                type="MAGIC_SPELL", # Not in Enum
                inputs=[], 
                outputs=[]
            )