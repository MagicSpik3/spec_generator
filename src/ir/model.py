from pydantic import BaseModel, Field, model_validator
from src.ir.types import DataType, OpType
from typing import Dict, List, Tuple, Union, Any
import networkx as nx

class Dataset(BaseModel):
    id: str
    source: str
    columns: List[Tuple[str, DataType]] = Field(default_factory=list)

    @property
    def column_names(self) -> List[str]:
        return [c[0] for c in self.columns]

class Operation(BaseModel):
    id: str
    type: OpType
    inputs: List[str]
    outputs: List[str]
    # Allow complex params (Lists, Unions) for Aggregates/Joins
    params: Dict[str, Union[str, int, float, bool, List[str]]] = Field(default_factory=dict)

class Pipeline(BaseModel):
    datasets: List[Dataset]
    operations: List[Operation]

    @model_validator(mode='after')
    def validate_integrity(self):
        """
        Validates the DAG structure using NetworkX.
        Runs automatically on initialization.
        """
        G = nx.DiGraph()
        
        # Create lookup for fast validation
        ds_ids = set(d.id for d in self.datasets)
        
        # Add Nodes
        for ds in self.datasets:
            G.add_node(ds.id, type="dataset")
        
        for op in self.operations:
            G.add_node(op.id, type="operation")
            
            # Check inputs exist
            for inp in op.inputs:
                if inp not in ds_ids:
                    raise ValueError(f"Operation {op.id} references unknown input Dataset '{inp}'")
                G.add_edge(inp, op.id)
            
            # Check outputs exist
            for out in op.outputs:
                if out not in ds_ids:
                    raise ValueError(f"Operation {op.id} references unknown output Dataset '{out}'")
                G.add_edge(op.id, out)

        if not nx.is_directed_acyclic_graph(G):
            # cycle = nx.find_cycle(G) # Optional: detailed cycle info
            raise ValueError("Pipeline Validation Failed: Cycle detected in graph")
            
        return self