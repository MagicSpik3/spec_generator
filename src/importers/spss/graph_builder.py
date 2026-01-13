import hashlib
from platform import node
from typing import List, Optional, Tuple
from src.importers.spss.ast import AggregateNode, AstNode, DataListNode, FilterNode, JoinNode, LoadNode, ComputeNode, MaterializeNode, SaveNode, GenericNode
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
            elif isinstance(node, FilterNode): # 🟢 New
                self._handle_filter(node)
            elif isinstance(node, MaterializeNode): # 🟢 New
                self._handle_materialize(node)
            elif isinstance(node, SaveNode):
                self._handle_save(node)
            elif isinstance(node, GenericNode):
                self._handle_generic(node)
            elif isinstance(node, JoinNode):
                self._handle_join(node)
            elif isinstance(node, DataListNode):
                self._handle_data_list(node)
            elif isinstance(node, AggregateNode):
                self._handle_aggregate(node)

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


    def _handle_filter(self, node: FilterNode):
        if not self.active_dataset_id: return

        new_ds_id = self._get_next_ds_id("filtered")
        # Filter keeps same schema
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("filter"),
            type=OpType.FILTER,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            params={'condition': node.condition}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    def _handle_materialize(self, node: MaterializeNode):
        if not self.active_dataset_id: return

        # EXECUTE is a barrier. It doesn't change schema, but forces evaluation.
        # In our graph, we represent this as a checkpoint.
        new_ds_id = self._get_next_ds_id("materialized")
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("exec"),
            type=OpType.MATERIALIZE,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            params={}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id            


    def _handle_join(self, node: JoinNode):
        # 1. Resolve Inputs
        input_ids = []
        for src in node.sources:
            if src == "*":
                if self.active_dataset_id:
                    input_ids.append(self.active_dataset_id)
                else:
                    # Edge case: * used with no active state
                    pass 
            else:
                # Check if we already loaded this file? 
                # For MVP, assume it refers to a file we need to treat as a source
                # In a real compiler, we might look up 'ds_rates.sav' if it was loaded earlier.
                # Here we create an implicit reference.
                file_ds_id = f"source_{src}"
                # If not exists, register it (Simple check)
                if not any(d.id == file_ds_id for d in self.datasets):
                     self.datasets.append(Dataset(id=file_ds_id, source="file"))
                input_ids.append(file_ds_id)

        # 2. Create Output Dataset
        new_ds_id = self._get_next_ds_id("joined")
        
        # Schema Merging Strategy (Naive):
        # Start with columns from the first input (Left Join logic usually)
        # In reality, MATCH FILES is a full outer join of variables on the key.
        # We will copy the active columns for now.
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("join"),
            type=OpType.JOIN,
            inputs=input_ids,
            outputs=[new_ds_id],
            params={'by': ", ".join(node.by)}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id


    def _handle_aggregate(self, node: AggregateNode):
        if not self.active_dataset_id: return

        if node.outfile == "*" or node.outfile == "":
             new_ds_id = self._get_next_ds_id("agg_active")
             is_side_effect = False
        else:
             new_ds_id = f"source_{node.outfile}"
             is_side_effect = True

        # 🟢 CALCULATE OUTPUT SCHEMA
        # 1. Start with Break Variables (Inherit types from input if possible)
        new_cols = []
        
        # Lookup input dataset to find types of break vars
        input_ds = next((d for d in self.datasets if d.id == self.active_dataset_id), None)
        input_schema = dict(input_ds.columns) if input_ds else {}

        for break_var in node.break_vars:
            # Default to String if unknown, or lookup
            # In a real compiler, we'd do strictly typed lookup. 
            # For now, simplistic inheritance is enough.
            dtype = input_schema.get(break_var, DataType.UNKNOWN)
            new_cols.append((break_var, dtype))

        # 2. Add Aggregation Targets
        for agg_expr in node.aggregations:
            # expr format: "target = FUNC(src)"
            if "=" in agg_expr:
                target = agg_expr.split("=")[0].strip()
                # Aggregations (SUM, MEAN, N) are almost always numeric
                new_cols.append((target, DataType.INTEGER))

        # Create Dataset with calculated schema
        new_ds = Dataset(id=new_ds_id, source="derived", columns=new_cols)
        self.datasets.append(new_ds)

        # Create Operation
        op = Operation(
            id=self._get_next_op_id("aggregate"),
            type=OpType.AGGREGATE,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            params={
                'outfile': node.outfile,
                'break': node.break_vars,
                'aggregations': node.aggregations
            }
        )
        self.operations.append(op)
        
        if not is_side_effect:
            self.active_dataset_id = new_ds_id

    def _handle_data_list(self, node: DataListNode):
        """
        Register a new dataset created by inline data.
        """
        # Create a new dataset ID
        new_ds_id = self._get_next_ds_id("inline")
        
        # Register it with the explicit schema
        new_ds = Dataset(id=new_ds_id, source="inline", columns=node.columns)
        self.datasets.append(new_ds)
        
        # Create a 'Load' operation for it
        op = Operation(
            id=self._get_next_op_id("load_inline"),
            type=OpType.LOAD,
            inputs=[],
            outputs=[new_ds_id],
            params={'source_type': 'inline'}
        )
        self.operations.append(op)
        
        # Set as active
        self.active_dataset_id = new_ds_id