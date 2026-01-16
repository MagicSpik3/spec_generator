from typing import List
from spec_generator.importers.spss.parsers.base import BaseParserMixin
from spec_generator.importers.spss.tokens import TokenType
from spec_generator.importers.spss.ast import RecodeNode, SortNode

class LogicParserMixin(BaseParserMixin):
    
    def parse_recode(self) -> RecodeNode:
        self.advance() # Skip RECODE
        
        # 1. Capture Source Variables
        source_vars = []
        while self.current_token().type == TokenType.IDENTIFIER:
            # Avoid capturing keywords like INTO if user forgot parens (unlikely but safe)
            if self.current_token().value.upper() == "INTO":
                break
            source_vars.append(self.current_token().value)
            self.advance()
            
        # 2. Skip the mapping rules until 'INTO' or terminator
        mapping_logic = []
        target_vars = []
        
        while self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            
            # Detect the structural keyword "INTO"
            if t.type == TokenType.IDENTIFIER and t.value.upper() == "INTO":
                self.advance() # Skip INTO
                
                # Now capture targets
                while self.current_token().type == TokenType.IDENTIFIER:
                    target_vars.append(self.current_token().value)
                    self.advance()
                break # We usually stop parsing after targets
                
            mapping_logic.append(t.value)
            self.advance()
            
        self.advance() # Skip terminator
        
        # If no INTO, targets = sources (In-place update)
        if not target_vars:
            target_vars = source_vars[:]
            
        return RecodeNode(
            source_vars=source_vars,
            target_vars=target_vars,
            map_logic=" ".join(mapping_logic)
        )
    
    def _parse_sort(self) -> SortNode:
        # Consumes: SORT CASES [BY] var1 var2 ...
        self.advance() # Skip 'SORT' token
        
        # Skip optional keywords 'CASES' and 'BY'
        if self.current_token().value.upper() == "CASES":
            self.advance()
        if self.current_token().value.upper() == "BY":
            self.advance()
            
        keys = []
        # Collect identifiers until we hit a terminator or unknown token
        while self.current_token().type == TokenType.IDENTIFIER:
            keys.append(self.current_token().value)
            self.advance()
            
        self.advance() # Skip Terminator (.)
        return SortNode(keys=keys)