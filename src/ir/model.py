import networkx as nx
from typing import List, Dict, Optional, Tuple, Set
from pydantic import BaseModel, Field, model_validator
from src.ir.types import DataType, OpType

class Dataset(BaseModel):
    id: str
    source: str = "derived"
    columns: List[Tuple[str, DataType]] = Field(default_factory=list)

    @property
    def column_names(self) -> List[str]:
        return [c[0] for c in self.columns]

class Operation(BaseModel):
    id: str
    type: OpType
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    params: Dict[str, str] = Field(default_factory=dict)

class Pipeline(BaseModel):
    datasets: List[Dataset]
    operations: List[Operation]

    @model_validator(mode='after')
    def validate_integrity(self) -> 'Pipeline':
        """
        Validates the pipeline structure using NetworkX.
        """
        # 1. Build Registry for O(1) lookups
        dataset_ids = {ds.id for ds in self.datasets}
        
        # 2. Build the Graph
        # Nodes = Datasets
        # Edges = Operations (Input -> Output)
        G = nx.DiGraph()
        G.add_nodes_from(dataset_ids)

        for op in self.operations:
            # Check for unknown datasets
            for inp in op.inputs:
                if inp not in dataset_ids:
                    raise ValueError(f"Operation '{op.id}' references unknown input Dataset '{inp}'")
            for out in op.outputs:
                if out not in dataset_ids:
                    raise ValueError(f"Operation '{op.id}' references unknown output Dataset '{out}'")
            
            # Add edges to the graph representing data flow
            for inp in op.inputs:
                for out in op.outputs:
                    G.add_edge(inp, out, operation=op.id)

        # 3. Check for Cycles
        if not nx.is_directed_acyclic_graph(G):
            # Find the cycle for a helpful error message
            try:
                cycle = nx.find_cycle(G, orientation='original')
                raise ValueError(f"Cycle detected in pipeline logic: {cycle}")
            except nx.NetworkXNoCycle:
                pass # Should not happen given is_directed_acyclic_graph check

        return self