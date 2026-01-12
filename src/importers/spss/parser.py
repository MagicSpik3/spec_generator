from typing import List, Dict, Tuple
from src.importers.spss.lexer import SpssLexer
from src.importers.spss.tokens import Token, TokenType
from src.importers.spss.ast import AstNode, FilterNode, JoinNode, LoadNode, ComputeNode, MaterializeNode, SaveNode, GenericNode
from src.ir.types import DataType

class SpssParser:
    def __init__(self):
        self.lexer = SpssLexer()
        self.tokens: List[Token] = []
        self.pos = 0



    def parse(self, code: str) -> List[AstNode]:
        self.tokens = self.lexer.tokenize(code)
        self.pos = 0
        nodes = []

        while self.pos < len(self.tokens):
            token = self.current_token()
            
            # Dispatch
            if token.value == "GET" and self.peek_token(1).value == "DATA":
                nodes.append(self._parse_get_data())
            elif token.value == "COMPUTE":
                nodes.append(self._parse_compute())
            elif token.value == "SAVE":
                nodes.append(self._parse_save())
            
            # Semantic Commands
            elif token.type == TokenType.COMMAND and "SELECT IF" in token.value.upper():
                nodes.append(self._parse_select_if())
            elif token.type == TokenType.COMMAND and "EXECUTE" in token.value.upper():
                nodes.append(self._parse_execute())
            elif token.type == TokenType.COMMAND and "MATCH FILES" in token.value.upper():
                nodes.append(self._parse_match_files())
                
            # 🟢 New Handler for Inline Data
            elif token.type == TokenType.COMMAND and "BEGIN DATA" in token.value.upper():
                self._skip_data_block()
            
            elif token.type == TokenType.TERMINATOR:
                self.advance()
            else:
                nodes.append(self._parse_generic_command())
        
        return nodes

    # --------------------------------------------------------------------------
    # Node Parsers
    # --------------------------------------------------------------------------

    def _parse_get_data(self) -> LoadNode:
        # GET DATA ...
        self.advance(); self.advance() 
        params = self._collect_params_until_terminator()
        
        filename = params.get('/FILE', 'unknown').strip("'").strip('"')
        fmt = params.get('/TYPE', 'TXT')
        
        columns = []
        if '/VARIABLES' in params:
            columns = self._parse_variables_block(params['/VARIABLES'])

        return LoadNode(filename=filename, file_type=fmt, columns=columns)
    

    def _parse_select_if(self) -> FilterNode:
        self.advance() # Skip SELECT IF token
        
        condition = ""
        while self.current_token().type != TokenType.TERMINATOR:
            condition += self.current_token().value + " "
            self.advance()
        self.advance() # Skip dot
        
        return FilterNode(condition=condition.strip())

    def _parse_execute(self) -> MaterializeNode:
        self.advance() # Skip EXECUTE token
        if self.current_token().type == TokenType.TERMINATOR:
            self.advance() # Skip dot
        return MaterializeNode()

    def _parse_compute(self) -> ComputeNode:
        # COMPUTE x = 1.
        self.advance() # Skip COMPUTE
        
        # 1. Extract Target
        target_token = self.current_token()
        if target_token.type != TokenType.IDENTIFIER:
            raise SyntaxError(f"Expected Identifier after COMPUTE, got {target_token.value}")
        target = target_token.value
        self.advance()
        
        # 2. Extract Equals
        if self.current_token().type != TokenType.EQUALS:
             raise SyntaxError("Expected '=' in COMPUTE command")
        self.advance()

        # 3. Extract Expression (Until terminator)
        expression = ""
        while self.current_token().type != TokenType.TERMINATOR:
            expression += self.current_token().value + " "
            self.advance()
        self.advance() 
        
        return ComputeNode(target=target, expression=expression.strip())

    def _parse_save(self) -> SaveNode:
        # SAVE OUTFILE='...'
        self.advance() 
        params = self._collect_params_until_terminator()
        # Handle both OUTFILE and /OUTFILE
        outfile = params.get('OUTFILE', params.get('/OUTFILE', 'unknown')).strip("'").strip('"')
        return SaveNode(filename=outfile)

    def _parse_generic_command(self) -> GenericNode:
        cmd = self.current_token().value
        self.advance()
        # Consume until dot
        while self.pos < len(self.tokens) and self.current_token().type != TokenType.TERMINATOR:
            self.advance()
        self.advance()
        return GenericNode(command=cmd)

    # --------------------------------------------------------------------------
    # Helpers
    # --------------------------------------------------------------------------

    def _collect_params_until_terminator(self) -> Dict[str, str]:
        """
        Collects key-value pairs until the command terminator (dot).
        Handles keys that are Subcommands (/KEY) OR Identifiers followed by equals (KEY =).
        """
        params = {}
        current_key = None
        buffer = []

        while self.pos < len(self.tokens) and self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            next_t = self.peek_token(1)

            # 🟢 Logic Upgrade: Detect Keys
            is_subcommand = (t.type == TokenType.SUBCOMMAND)
            is_implicit_key = (t.type == TokenType.IDENTIFIER and next_t.type == TokenType.EQUALS)

            if is_subcommand or is_implicit_key:
                # Flush previous buffer to previous key
                if current_key: 
                    params[current_key] = " ".join(buffer).strip()
                
                current_key = t.value
                buffer = []
            
            elif t.type == TokenType.EQUALS:
                pass # Skip equals signs
            else:
                buffer.append(t.value)
            
            self.advance()
        
        # Flush final buffer
        if current_key: 
            params[current_key] = " ".join(buffer).strip()
            
        self.advance() # Consume terminator
        return params


    def _parse_variables_block(self, block_text: str) -> List[Tuple[str, DataType]]:
        block_tokens = self.lexer.tokenize(block_text + ".")
        columns = []
        i = 0
        while i < len(block_tokens) - 1:
            t_name = block_tokens[i]
            t_type = block_tokens[i+1]
            
            if t_name.type == TokenType.IDENTIFIER:
                col_type = DataType.UNKNOWN
                type_val = t_type.value.upper()
                
                # 🟢 FIX: Prioritize DATE check over 'A' check to prevent false positives
                # Also consider checking startswith for better accuracy
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


    def current_token(self) -> Token:
        if self.pos >= len(self.tokens): return Token(TokenType.TERMINATOR, ".", -1, -1)
        return self.tokens[self.pos]

    def peek_token(self, offset: int) -> Token:
        if self.pos + offset >= len(self.tokens): return Token(TokenType.TERMINATOR, ".", -1, -1)
        return self.tokens[self.pos + offset]

    def advance(self):
        self.pos += 1

    def _parse_match_files(self) -> JoinNode:
        self.advance() # Skip MATCH FILES
        
        sources = []
        by_keys = []
        
        # Custom loop to handle repeating /FILE keys
        while self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            
            if t.type == TokenType.SUBCOMMAND:
                sub_cmd = t.value.upper()
                
                if sub_cmd == "/FILE":
                    # Expect '=' then value
                    self.advance() # Skip /FILE
                    if self.current_token().type == TokenType.EQUALS:
                        self.advance() # Skip =
                    
                    val = self.current_token().value.strip("'").strip('"')
                    sources.append(val)
                    self.advance()
                    
                elif sub_cmd == "/BY":
                    # Expect '=' (optional in some dialects, strictly usually yes) 
                    # then list of identifiers
                    self.advance() # Skip /BY
                    if self.current_token().type == TokenType.EQUALS:
                        self.advance()
                    
                    # Capture all identifiers until next subcommand or dot
                    while self.current_token().type == TokenType.IDENTIFIER:
                        by_keys.append(self.current_token().value)
                        self.advance()
                else:
                    # Ignore other subcommands like /IN or /MAP for MVP
                    self.advance()
            else:
                self.advance()
                
        self.advance() # Skip terminator
        return JoinNode(sources=sources, by=by_keys)
    

    def _skip_data_block(self):
        """
        Consumes tokens until END DATA is found. 
        """
        self.advance() # Consume BEGIN DATA
        
        while self.pos < len(self.tokens):
            token = self.current_token()
            
            # Check for the exit condition
            if token.type == TokenType.COMMAND and "END DATA" in token.value.upper():
                self.advance() # Consume END DATA
                
                # Consume optional trailing dot
                if self.current_token().type == TokenType.TERMINATOR:
                    self.advance()
                return
            
            # Otherwise, swallow the token (it's just data)
            self.advance()