import hashlib
from typing import List, Optional, Tuple

# 🟢 Cleaned Import: Removed 'from platform import node'
from spec_generator.importers.spss.ast import (
    AggregateNode, AstNode, DataListNode, FilterNode, JoinNode,
    LoadNode, ComputeNode, MaterializeNode, RecodeNode, SaveNode, GenericNode, IgnorableNode, SortNode
)
from etl_ir.model import Pipeline, Dataset, Operation, Column
from etl_ir.types import DataType, OpType


class GraphBuilder:
    def __init__(self, metadata: dict = None):
        self.metadata = metadata or {}
        self.datasets: List[Dataset] = []
        self.operations: List[Operation] = []
        self.active_dataset_id: Optional[str] = None
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
            # 🟢 IMPROVEMENT: Check noise first for speed and clarity
            if isinstance(node, IgnorableNode):
                continue
            
            if isinstance(node, LoadNode):
                self._handle_load(node)
            elif isinstance(node, ComputeNode):
                self._handle_compute(node)
            elif isinstance(node, FilterNode): 
                self._handle_filter(node)
            elif isinstance(node, MaterializeNode): 
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
            elif isinstance(node, RecodeNode):
                self._handle_recode(node)
            elif isinstance(node, SortNode):
                self._handle_sort(node)

        return Pipeline(
            metadata=self.metadata,
            datasets=self.datasets, 
            operations=self.operations
        )

    def _get_active_columns(self) -> List[Column]:
        """Helper to fetch columns from the currently active dataset."""
        if not self.active_dataset_id:
            return []
        
        for ds in self.datasets:
            if ds.id == self.active_dataset_id:
                return ds.columns.copy()
        return []

    def _handle_load(self, node: LoadNode):
        dataset_id = f"source_{node.filename}"
        
        # 🟢 FIX: Convert AST Tuples to IR Column Objects
        # This prevents crashes later when accessing .name or .type
        ir_columns = [
            Column(name=col[0], type=col[1]) if isinstance(col, tuple) else col 
            for col in node.columns
        ]

        ds = Dataset(id=dataset_id, source="file", columns=ir_columns)
        self.datasets.append(ds)
        
        op = Operation(
            id=self._get_next_op_id("load"),
            type=OpType.LOAD_CSV,
            inputs=[],
            outputs=[dataset_id],
            parameters={'filename': node.filename, 'format': node.file_type}
        )
        self.operations.append(op)
        self.active_dataset_id = dataset_id

    def _handle_data_list(self, node: DataListNode):
        new_ds_id = self._get_next_ds_id("inline")
        
        # 🟢 FIX: Convert AST Tuples to IR Column Objects
        ir_columns = [
            Column(name=col[0], type=col[1]) if isinstance(col, tuple) else col 
            for col in node.columns
        ]

        new_ds = Dataset(id=new_ds_id, source="inline", columns=ir_columns)
        self.datasets.append(new_ds)
        
        op = Operation(
            id=self._get_next_op_id("load_inline"),
            type=OpType.LOAD_CSV,
            inputs=[],
            outputs=[new_ds_id],
            parameters={'source_type': 'inline'}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    # ... (Rest of your methods: _handle_compute, _handle_save, etc. are correct) ...
    
    def _handle_compute(self, node: ComputeNode):
        new_ds_id = self._get_next_ds_id("derived")
        new_columns = self._get_active_columns()
        
        existing_idx = next((i for i, c in enumerate(new_columns) if c.name == node.target), -1)
        new_col_def = Column(name=node.target, type=DataType.INTEGER)
        
        if existing_idx >= 0:
            new_columns[existing_idx] = new_col_def
        else:
            new_columns.append(new_col_def)

        new_ds = Dataset(id=new_ds_id, source="derived", columns=new_columns)
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("compute"),
            type=OpType.COMPUTE_COLUMNS,
            inputs=[self.active_dataset_id] if self.active_dataset_id else [],
            outputs=[new_ds_id],
            parameters={'target': node.target, 'expression': node.expression}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    def _handle_save(self, node: SaveNode):
        clean_filename = node.filename.strip("'").strip('"')
        file_ds_id = f"file_{clean_filename}"
        
        ds = Dataset(id=file_ds_id, source="file", columns=self._get_active_columns())
        self.datasets.append(ds)

        op = Operation(
            id=self._get_next_op_id("save"),
            type=OpType.SAVE_BINARY,
            inputs=[self.active_dataset_id] if self.active_dataset_id else [],
            outputs=[file_ds_id],
            parameters={'filename': clean_filename}
        )
        self.operations.append(op)

    def _handle_generic(self, node: GenericNode):
        if self.active_dataset_id:
            new_ds_id = self._get_next_ds_id("generic")
            new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
            self.datasets.append(new_ds)
            
            op = Operation(
                id=self._get_next_op_id("generic"),
                type=OpType.GENERIC_TRANSFORM,
                inputs=[self.active_dataset_id],
                outputs=[new_ds_id],
                parameters={'command': node.command}
            )
            self.operations.append(op)
            self.active_dataset_id = new_ds_id

    def _handle_filter(self, node: FilterNode):
        if not self.active_dataset_id: return

        new_ds_id = self._get_next_ds_id("filtered")
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("filter"),
            type=OpType.FILTER_ROWS,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            parameters={'condition': node.condition}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    def _handle_materialize(self, node: MaterializeNode):
        if not self.active_dataset_id: return

        new_ds_id = self._get_next_ds_id("materialized")
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("exec"),
            type=OpType.MATERIALIZE,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            parameters={}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id            

    def _handle_join(self, node: JoinNode):
        input_ids = []
        for src in node.sources:
            if src == "*":
                if self.active_dataset_id:
                    input_ids.append(self.active_dataset_id)
            else:
                clean_src = src.strip("'").strip('"')
                potential_internal_id = f"file_{clean_src}"
                internal_match = next((d for d in self.datasets if d.id == potential_internal_id), None)
                
                if internal_match:
                    input_ids.append(internal_match.id)
                else:
                    file_ds_id = f"source_{clean_src}"
                    if not any(d.id == file_ds_id for d in self.datasets):
                          self.datasets.append(Dataset(id=file_ds_id, source="file"))
                    input_ids.append(file_ds_id)

        new_ds_id = self._get_next_ds_id("joined")
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("join"),
            type=OpType.JOIN,
            inputs=input_ids,
            outputs=[new_ds_id],
            parameters={'by': ", ".join(node.by)}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id

    def _handle_aggregate(self, node: AggregateNode):
        if node.outfile == "*" or node.outfile == "":
             new_ds_id = self._get_next_ds_id("agg_active")
             is_side_effect = False
        else:
             new_ds_id = f"source_{node.outfile}"
             is_side_effect = True

        new_cols = []
        input_ds = next((d for d in self.datasets if d.id == self.active_dataset_id), None)
        # Using .get() here is safe now that input_ds.columns are Objects, not Tuples!
        input_schema = {c.name: c.type for c in input_ds.columns} if input_ds else {}

        for break_var in node.break_vars:
            dtype = input_schema.get(break_var, DataType.UNKNOWN)
            new_cols.append(Column(name=break_var, type=dtype))

        for agg_expr in node.aggregations:
            if "=" in agg_expr:
                target = agg_expr.split("=")[0].strip()
                new_cols.append(Column(name=target, type=DataType.INTEGER))

        new_ds = Dataset(id=new_ds_id, source="derived", columns=new_cols)
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("aggregate"),
            type=OpType.AGGREGATE,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            parameters={
                'outfile': node.outfile,
                'break': node.break_vars,
                'aggregations': node.aggregations
            }
        )
        self.operations.append(op)
        
        if not is_side_effect:
            self.active_dataset_id = new_ds_id

    def _handle_recode(self, node: RecodeNode):
        input_ds = next((d for d in self.datasets if d.id == self.active_dataset_id), None)
        new_cols = list(input_ds.columns) if input_ds else []
        existing_names = {c.name.upper() for c in new_cols}
        
        for target in node.target_vars:
            if target.upper() not in existing_names:
                new_cols.append(Column(name=target, type=DataType.UNKNOWN))

        new_ds_id = self._get_next_ds_id("recode")
        new_ds = Dataset(id=new_ds_id, source="derived", columns=new_cols)
        self.datasets.append(new_ds)
        
        self.operations.append(Operation(
            id=self._get_next_op_id("recode"),
            type=OpType.COMPUTE_COLUMNS,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            parameters={'logic': node.map_logic}
        ))
        self.active_dataset_id = new_ds_id

    def _handle_sort(self, node: SortNode):
        if not self.active_dataset_id: return

        new_ds_id = self._get_next_ds_id("sorted")
        # Sorting doesn't change columns, so we inherit schema
        new_ds = Dataset(id=new_ds_id, source="derived", columns=self._get_active_columns())
        self.datasets.append(new_ds)

        op = Operation(
            id=self._get_next_op_id("sort"),
            type=OpType.SORT_ROWS,
            inputs=[self.active_dataset_id],
            outputs=[new_ds_id],
            # Join list into "col1, col2" string for RGenerator
            parameters={'keys': ", ".join(node.keys)}
        )
        self.operations.append(op)
        self.active_dataset_id = new_ds_id