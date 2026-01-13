from typing import List, Dict, Optional
from src.importers.spss.tokens import Token, TokenType
from src.importers.spss.lexer import SpssLexer

class BaseParserMixin:
    """
    Provides core navigation methods (advance, peek, match) for the Parser.
    Expected to be mixed into a class that has 'self.tokens' and 'self.pos'.
    """
    
    def __init__(self):
        # These will be populated by the main SpssParser.parse() method
        self.tokens: List[Token] = []
        self.pos = 0
        self.lexer = SpssLexer() # Helper for parsing sub-blocks

    def current_token(self) -> Token:
        if self.pos >= len(self.tokens): 
            return Token(TokenType.TERMINATOR, ".", -1, -1)
        return self.tokens[self.pos]

    def peek_token(self, offset: int) -> Token:
        if self.pos + offset >= len(self.tokens): 
            return Token(TokenType.TERMINATOR, ".", -1, -1)
        return self.tokens[self.pos + offset]

    def advance(self):
        self.pos += 1

    def _collect_params_until_terminator(self) -> Dict[str, str]:
        """
        Helper: Collects key-value pairs (e.g. /FILE='x' or /BREAK=y) until a dot.
        """
        params = {}
        current_key = None
        buffer = []

        while self.pos < len(self.tokens) and self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            next_t = self.peek_token(1)

            # Detect Keys: Subcommands (/KEY) OR Identifiers followed by equals (KEY =)
            is_subcommand = (t.type == TokenType.SUBCOMMAND)
            is_implicit_key = (t.type == TokenType.IDENTIFIER and next_t.type == TokenType.EQUALS)

            if is_subcommand or is_implicit_key:
                if current_key: 
                    params[current_key] = " ".join(buffer).strip()
                current_key = t.value
                buffer = []
            elif t.type == TokenType.EQUALS:
                pass 
            else:
                buffer.append(t.value)
            
            self.advance()
        
        if current_key: 
            params[current_key] = " ".join(buffer).strip()
            
        self.advance() # Consume terminator
        return params