from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from src.ir.types import DataType

@dataclass
class AstNode:
    raw_command: str = "UNKNOWN" 

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