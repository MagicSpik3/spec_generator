import re
from typing import List
from src.importers.spss.tokens import Token, TokenType
from src.importers.spss.grammar import SPSS_TOKENS

class SpssLexer:
    """
    Tokenizer for SPSS Syntax.
    Robustly handles 'Start of Line' context for comments.
    """
    
    def tokenize(self, code: str) -> List[Token]:
        tokens = []
        code = code.replace("\r\n", "\n")
        
        pos = 0
        line_num = 1
        col_num = 1
        at_line_start = True # 🟢 New State Flag
        
        while pos < len(code):
            match = None
            
            # 1. Skip Whitespace
            whitespace = re.match(r"\s+", code[pos:])
            if whitespace:
                chunk = whitespace.group(0)
                lines_in_chunk = chunk.count('\n')
                line_num += lines_in_chunk
                
                if lines_in_chunk > 0:
                    col_num = 1
                    at_line_start = True # 🟢 Reset flag on newline
                else:
                    col_num += len(chunk)
                    # Note: Spaces don't reset 'at_line_start'. 
                    # If we are at start, space keeps us at start.
                
                pos += len(chunk)
                continue

            # 2. Try match against Grammar
            for token_type, pattern in SPSS_TOKENS:
                match = pattern.match(code, pos)
                if match:
                    # 🟢 Context Check for Comments
                    if token_type == TokenType.COMMENT:
                        if not at_line_start:
                            # False positive: we matched '*' as a comment, 
                            # but we are in the middle of a line (e.g. 2 * 3).
                            # Reject this match, let loop continue to OPERATOR.
                            match = None
                            continue
                    
                    text = match.group(0)
                    
                    # Store valid tokens (Skip comments)
                    if token_type != TokenType.COMMENT:
                        tokens.append(Token(token_type, text, line_num, col_num))
                        at_line_start = False # 🟢 We consumed a real token
                    
                    # Advance
                    pos += len(text)
                    col_num += len(text)
                    break
            
            # 3. Handle Invalid Characters
            if not match:
                char = code[pos]
                raise SyntaxError(
                    f"Unexpected character '{char}' at line {line_num}, col {col_num}"
                )
                
        return tokens
        
    def normalize_command(self, raw_cmd: str) -> str:
        return " ".join(raw_cmd.split())