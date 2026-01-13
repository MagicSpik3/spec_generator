from typing import List, Tuple
from src.importers.spss.parsers.base import BaseParserMixin
from src.importers.spss.tokens import TokenType
from src.importers.spss.ast import DataListNode
from src.ir.types import DataType

class SchemaParserMixin(BaseParserMixin):
    """
    Handles commands that define dataset structure: DATA LIST, VARIABLES.
    """

    def parse_data_list(self) -> DataListNode:
        self.advance() # Skip DATA
        self.advance() # Skip LIST
        
        # Skip optional keywords (FREE, LIST, etc) until '/'
        while self.current_token().type != TokenType.SUBCOMMAND and \
              self.current_token().type != TokenType.TERMINATOR and \
              self.current_token().value != "/":
             self.advance()
        
        columns = []
        if self.current_token().value == "/":
            self.advance() # Skip slash
            
            # 🟢 ROBUST PAREN REMOVAL LOGIC
            var_block_tokens = []
            while self.current_token().type != TokenType.TERMINATOR:
                t = self.current_token()
                if t.type != TokenType.LPAREN and t.type != TokenType.RPAREN:
                    var_block_tokens.append(t.value)
                self.advance()
            
            block_str = " ".join(var_block_tokens)
            columns = self._parse_variables_block(block_str)
            
        self.advance() # Skip terminator
        return DataListNode(columns=columns)

    def _parse_variables_block(self, block_text: str) -> List[Tuple[str, DataType]]:
        """
        Parses "name type name type" strings.
        """
        # We need a fresh lexer for this block logic
        block_tokens = self.lexer.tokenize(block_text + ".")
        columns = []
        i = 0
        while i < len(block_tokens) - 1:
            t_name = block_tokens[i]
            t_type = block_tokens[i+1]
            
            if t_name.type == TokenType.IDENTIFIER:
                col_type = DataType.UNKNOWN
                type_val = t_type.value.upper()
                
                if "DATE" in type_val: 
                    col_type = DataType.DATE
                elif type_val.startswith("F") or "NUM" in type_val: 
                    col_type = DataType.INTEGER
                elif type_val.startswith("A") or "STR" in type_val: 
                    col_type = DataType.STRING
                
                columns.append((t_name.value, col_type))
                i += 2
            else:
                i += 1
        return columns