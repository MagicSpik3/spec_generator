from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from etl_ir.types import DataType



@dataclass
class AstNode:
    raw_command: str = "UNKNOWN" 

@dataclass
class IgnorableNode(AstNode):
    """Represents a command that has NO impact on the data flow (Metadata/Reporting)"""
    # 🟢 Fix: Add default value = "" to satisfy inheritance rules
    command: str = "UNKNOWN"
    content: str = ""

@dataclass
class LoadNode(AstNode):
    filename: str = ""
    file_type: str = "TXT"
    columns: List[Tuple[str, DataType]] = field(default_factory=list)

@dataclass
class ComputeNode(AstNode):
    target: str = ""
    expression: str = ""

@dataclass
class FilterNode(AstNode): # 🟢 New
    condition: str = ""

@dataclass
class MaterializeNode(AstNode): # 🟢 New
    pass

@dataclass
class SaveNode(AstNode):
    filename: str = ""

@dataclass
class GenericNode(AstNode):
    command: str = ""
    params: Dict[str, str] = field(default_factory=dict)

@dataclass
class JoinNode(AstNode):
    """Represents MATCH FILES."""
    sources: List[str] = field(default_factory=list)
    by: List[str] = field(default_factory=list)    


@dataclass
class AggregateNode(AstNode):
    outfile: str = ""
    break_vars: List[str] = field(default_factory=list)
    aggregations: List[str] = field(default_factory=list) # e.g. ["mean_x = MEAN(x)"]    


@dataclass
class DataListNode(AstNode):
    columns: List[Tuple[str, DataType]] = field(default_factory=list)    


@dataclass
class RecodeNode(AstNode):
    source_vars: List[str] = field(default_factory=list)
    target_vars: List[str] = field(default_factory=list)
    map_logic: str = ""    

@dataclass
class SortNode(AstNode):
    keys: List[str] = field(default_factory=list)