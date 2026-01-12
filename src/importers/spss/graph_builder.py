import hashlib
from typing import List, Optional, Tuple
from src.importers.spss.ast import AstNode, LoadNode, ComputeNode, SaveNode, GenericNode
from src.ir.model import Pipeline, Dataset, Operation
from src.ir.types import DataType, OpType

class GraphBuilder:
    def __init__(self):
        self.datasets: List[Dataset] = []
        self.operations: List[Operation] = []
        self.active_dataset_id: Optional[str] = None
        
        # 🟢 Determinism Helpers
        self.op_counter = 0
        self.ds_counter = 0

    def _get_next_op_id(self, prefix: str) -> str:
        self.op_counter += 1
        return f"op_{self.op_counter:03d}_{prefix}"

    def _get_next_ds_id(self, prefix: str) -> str:
        self.ds_counter += 1
        return f"ds_{self.ds_counter:03d}_{prefix}"

    def build(self, nodes: List[AstNode]) -> Pipeline:
        self.datasets = []
        self.operations = []
        self.active_dataset_id = None
        self.op_counter = 0
        self.ds_counter = 0

        for node in nodes:
            if isinstance(node, LoadNode):
                self._handle_load(node)
            elif isinstance(node, ComputeNode):
                self._handle_compute(node)
            elif isinstance(node, SaveNode):
                self._handle_save(node)
            elif isinstance(node, GenericNode):
                self._handle_generic(node)
        
        return Pipeline(datasets=self.datasets, operations=self.operations)

    def _get_active_columns(self) -> List[Tuple[str, DataType]]:
        """Helper to fetch columns from the currently active dataset."""
        if not self.active_dataset_id:
            return []
        
        # Find the dataset object
        # (In production, use a Dict for O(1) lookup)
        for ds in self.datasets:
            if ds.id == self.active_dataset_id:
                return ds.columns.copy() # Return copy to prevent mutation issues
        return []

    def _handle_load(self, node: LoadNode):
        dataset_id = f"source_{node.filename}"
        ds = Dataset(id=dataset_id, source="file", columns=node.columns)
        self.datasets.append(ds)
        
        op = Operation(
            id=self._get_next_op_id("load"),
            type=OpType.LOAD,
            inputs=[],
            outputs=[dataset_id],
            params={'filename': node.filename, 'format': node.file_type}
        )
        self.operations.append(op)
        self.active_dataset_id = dataset_id

    def _handle_compute(self, node: ComputeNode):
        new_ds_id = self._get_next_ds_id("derived")
        
        # 🟢 Schema Propagation
        # 1. Start with existing columns
        new_columns = self._get_active_columns()
        
        # 2. Update or Append the target variable
        # Check if it already exists (mutation) or is new
        existing_idx = next((i for i, c in enumerate(new_columns) if c[0] == node.target), -1)
        
        # For now, we assume numeric type for computed vars (can be refined later)
        new_col_def = (node.target, DataType.INTEGER) 
        
        if existing_idx >= 0:
            new_columns[existing_idx] = new_col_def
        else:
            new_columns.append(new_col_def)

        new_ds = Dataset(id=new_ds_id, source="derived", columns=new_columns)
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("compute"),
            type=OpType.COMPUTE,
            inputs=[self.active_dataset_id] if self.active_dataset_id else [],
            outputs=[new_ds_id],
            params={'target': node.target, 'expression': node.expression}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    def _handle_save(self, node: SaveNode):
        file_ds_id = f"file_{node.filename}"
        # Output file inherits the schema of the active dataset!
        ds = Dataset(id=file_ds_id, source="file", columns=self._get_active_columns())
        self.datasets.append(ds)

        op = Operation(
            id=self._get_next_op_id("save"),
            type=OpType.SAVE,
            inputs=[self.active_dataset_id] if self.active_dataset_id else [],
            outputs=[file_ds_id],
            params={'filename': node.filename}
        )
        self.operations.append(op)

    def _handle_generic(self, node: GenericNode):
        if self.active_dataset_id:
            new_ds_id = self._get_next_ds_id("generic")
            # Generic commands pass schema through unchanged (conservative approach)
            new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
            self.datasets.append(new_ds)
            
            op = Operation(
                id=self._get_next_op_id("generic"),
                type=OpType.GENERIC,
                inputs=[self.active_dataset_id],
                outputs=[new_ds_id],
                params={'command': node.command}
            )
            self.operations.append(op)
            self.active_dataset_id = new_ds_id