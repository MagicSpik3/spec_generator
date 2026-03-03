from typing import List
from spec_generator.importers.spss.parsers.base import BaseParserMixin
from spec_generator.importers.spss.tokens import TokenType
from spec_generator.importers.spss.ast import IfNode, RecodeNode, SortNode

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
    
    def _parse_if(self) -> IfNode:
        # Syntax: IF (condition) target = expression.
        self.advance() # Skip 'IF'
        
        # 🟢 FIX: Properly handle conditions with assignment operators inside parens
        # Step 1: Extract the parenthesized condition (handles IF (x = y) z = expr)
        condition_tokens = []
        if self.current_token().value == '(':
            self.advance()  # Skip opening paren
            paren_depth = 1
            while paren_depth > 0 and self.current_token().type != TokenType.TERMINATOR:
                if self.current_token().value == '(':
                    paren_depth += 1
                elif self.current_token().value == ')':
                    paren_depth -= 1
                    if paren_depth == 0:
                        break  # Don't include closing paren
                condition_tokens.append(self.current_token())
                self.advance()
            
            if self.current_token().value != ')':
                raise SyntaxError("Expected ')' after IF condition")
            self.advance()  # Skip closing paren
            condition = " ".join([t.value for t in condition_tokens]).strip()
        else:
            raise SyntaxError("Expected '(' after IF keyword")
        
        # Step 2: Extract the target variable (next identifier after closing paren)
        if self.current_token().type != TokenType.IDENTIFIER:
            raise SyntaxError(f"Expected target variable after IF condition, got {self.current_token().value}")
        target = self.current_token().value
        self.advance()
        
        # Step 3: Expect '=' assignment operator
        if self.current_token().type != TokenType.EQUALS:
            raise SyntaxError(f"Expected '=' in IF assignment, got {self.current_token().value}")
        self.advance()
        
        # Step 4: Capture the expression until terminator
        expr_tokens = []
        while self.current_token().type != TokenType.TERMINATOR:
            expr_tokens.append(self.current_token().value)
            self.advance()
        
        expr = " ".join(expr_tokens).strip()
        self.advance()  # Skip terminator
        
        return IfNode(condition=condition, target=target, expression=expr)