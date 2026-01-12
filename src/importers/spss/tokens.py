from enum import Enum, auto
from dataclasses import dataclass

class TokenType(Enum):
    # Structural
    COMMAND = auto()      # COMPUTE, GET DATA, IF, MATCH FILES
    SUBCOMMAND = auto()   # /FILE, /VARIABLES, /TABLE
    TERMINATOR = auto()   # . (The dot at end of command)
    
    # Data
    IDENTIFIER = auto()   # variable_name, column_id
    STRING_LITERAL = auto() # "filename.csv", 'value'
    NUMBER_LITERAL = auto() # 123, 45.6
    
    # Operators & Punctuation
    EQUALS = auto()       # =
    LPAREN = auto()       # (
    RPAREN = auto()       # )
    COMMA = auto()        # ,
    OPERATOR = auto()     # +, -, *, /, AND, OR, GT, LT, EQ
    
    # Noise
    COMMENT = auto()      # * This is a comment

@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int