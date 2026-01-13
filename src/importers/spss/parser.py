from typing import List
from src.importers.spss.parsers.logic import LogicParserMixin
from src.importers.spss.tokens import TokenType
from src.importers.spss.ast import (
    AstNode, GenericNode, LoadNode, ComputeNode, 
    FilterNode, MaterializeNode, SaveNode, JoinNode
)
from src.importers.spss.parsers.base import BaseParserMixin
from src.importers.spss.parsers.schema import SchemaParserMixin
from src.importers.spss.parsers.stats import StatsParserMixin

class SpssParser(SchemaParserMixin, 
                 StatsParserMixin, 
                 LogicParserMixin, 
                 BaseParserMixin):
    
    def parse(self, code: str) -> List[AstNode]:
        self.tokens = self.lexer.tokenize(code)
        self.pos = 0
        nodes = []

        while self.pos < len(self.tokens):
            token = self.current_token()
            
            # --- Mixin Dispatch ---
            if token.value.upper() == "DATA" and self.peek_token(1).value.upper() == "LIST":
                nodes.append(self.parse_data_list())
            elif token.type == TokenType.COMMAND and "AGGREGATE" in token.value.upper():
                nodes.append(self.parse_aggregate())

            # --- Legacy Local Handlers ---
            elif token.value == "GET" and self.peek_token(1).value == "DATA":
                nodes.append(self._parse_get_data())
            elif token.value == "COMPUTE":
                nodes.append(self._parse_compute())
            elif token.value == "SAVE":
                nodes.append(self._parse_save())
            
            elif token.type == TokenType.COMMAND and "SELECT IF" in token.value.upper():
                nodes.append(self._parse_select_if())
            elif token.type == TokenType.COMMAND and "EXECUTE" in token.value.upper():
                nodes.append(self._parse_execute())
            elif token.type == TokenType.COMMAND and "MATCH FILES" in token.value.upper():
                nodes.append(self._parse_match_files())
                
            elif token.type == TokenType.COMMAND and "BEGIN DATA" in token.value.upper():
                self._skip_data_block()
            
            elif token.type == TokenType.TERMINATOR:
                self.advance()
            elif token.value.upper() == "RECODE":
                nodes.append(self.parse_recode())

            else:
                nodes.append(self._parse_generic_command())
        
        return nodes

    # --------------------------------------------------------------------------
    # Legacy Handlers
    # --------------------------------------------------------------------------

    def _parse_get_data(self) -> LoadNode:
        self.advance(); self.advance() 
        params = self._collect_params_until_terminator()
        filename = params.get('/FILE', 'unknown').strip("'").strip('"')
        columns = []
        if '/VARIABLES' in params:
            columns = self._parse_variables_block(params['/VARIABLES'])
        return LoadNode(filename=filename, file_type=params.get('/TYPE', 'TXT'), columns=columns)

    def _parse_compute(self) -> ComputeNode:
        self.advance() # Skip COMPUTE
        
        # 🟢 Validation 1: Check for Target Identifier
        if self.current_token().type != TokenType.IDENTIFIER:
             raise SyntaxError(f"Expected Identifier after COMPUTE, got {self.current_token().value}")
        target = self.current_token().value
        self.advance()
        
        # 🟢 Validation 2: Check for Equals Sign
        if self.current_token().type != TokenType.EQUALS:
             raise SyntaxError(f"Expected '=' in COMPUTE command, got {self.current_token().value}")
        self.advance() 

        expr = ""
        while self.current_token().type != TokenType.TERMINATOR:
            expr += self.current_token().value + " "
            self.advance()
        self.advance()
        return ComputeNode(target=target, expression=expr.strip())

    def _parse_select_if(self) -> FilterNode:
        self.advance(); cond = ""
        while self.current_token().type != TokenType.TERMINATOR:
            cond += self.current_token().value + " "; self.advance()
        self.advance()
        return FilterNode(condition=cond.strip())

    def _parse_execute(self) -> MaterializeNode:
        self.advance()
        if self.current_token().type == TokenType.TERMINATOR: self.advance()
        return MaterializeNode()

    def _parse_save(self) -> SaveNode:
        self.advance(); params = self._collect_params_until_terminator()
        return SaveNode(filename=params.get('OUTFILE', params.get('/OUTFILE', 'unknown')).strip("'").strip('"'))

    def _parse_match_files(self) -> JoinNode:
        self.advance(); sources = []; by_keys = []
        while self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            if t.type == TokenType.SUBCOMMAND:
                if t.value.upper() == "/FILE":
                    self.advance(); 
                    if self.current_token().type == TokenType.EQUALS: self.advance()
                    sources.append(self.current_token().value.strip("'").strip('"')); self.advance()
                elif t.value.upper() == "/BY":
                    self.advance(); 
                    if self.current_token().type == TokenType.EQUALS: self.advance()
                    while self.current_token().type == TokenType.IDENTIFIER:
                        by_keys.append(self.current_token().value); self.advance()
                else: self.advance()
            else: self.advance()
        self.advance()
        return JoinNode(sources=sources, by=by_keys)

    def _skip_data_block(self):
        self.advance()
        while self.pos < len(self.tokens):
            if self.current_token().type == TokenType.COMMAND and "END DATA" in self.current_token().value.upper():
                self.advance()
                if self.current_token().type == TokenType.TERMINATOR: self.advance()
                return
            self.advance()

    def _parse_generic_command(self) -> GenericNode:
        cmd = self.current_token().value; self.advance()
        while self.pos < len(self.tokens) and self.current_token().type != TokenType.TERMINATOR:
            self.advance()
        self.advance()
        return GenericNode(command=cmd)