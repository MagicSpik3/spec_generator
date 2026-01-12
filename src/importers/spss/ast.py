from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from src.ir.types import DataType



@dataclass
class AstNode:
    """Base class for all SPSS Abstract Syntax Tree nodes."""
    # Giving this a default prevents 'non-default argument follows default argument' errors
    raw_command: str = "UNKNOWN" 

@dataclass
class LoadNode(AstNode):
    filename: str = ""
    file_type: str = "TXT"
    columns: List[Tuple[str, DataType]] = field(default_factory=list)

@dataclass
class ComputeNode(AstNode):
    expression: str = ""
    target: str = ""      # 🟢 New Field

@dataclass
class SaveNode(AstNode):
    filename: str = ""

@dataclass
class GenericNode(AstNode):
    command: str = ""
    params: Dict[str, str] = field(default_factory=dict)
