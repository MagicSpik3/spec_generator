import pytest
from spec_generator.importers.spss.lexer import SpssLexer
from spec_generator.importers.spss.tokens import TokenType

class TestSpssLexer:
    
    def setup_method(self):
        self.lexer = SpssLexer()

    def test_basic_command_tokenization(self):
        """
        Verifies simple command splitting.
        UPDATED: Expects COMPUTE to be a COMMAND, not an IDENTIFIER.
        """
        code = "COMPUTE x = 1."
        tokens = self.lexer.tokenize(code)
        
        assert len(tokens) == 5
        # 🟢 CHANGE: We now expect the lexer to know this is a command
        assert tokens[0].type == TokenType.COMMAND 
        assert tokens[0].value.upper() == "COMPUTE"
        
        assert tokens[1].type == TokenType.IDENTIFIER # x
        assert tokens[2].type == TokenType.EQUALS     # =
        assert tokens[3].type == TokenType.NUMBER_LITERAL # 1
        assert tokens[4].type == TokenType.TERMINATOR # .

    def test_handles_quoted_strings(self):
        """
        SPSS allows both single and double quotes and escaped quotes.
        """
        code = "SAVE OUTFILE='data''s.sav'."
        tokens = self.lexer.tokenize(code)
        
        # [SAVE] [OUTFILE] [=] ['data''s.sav'] [.]
        assert tokens[2].type == TokenType.EQUALS        
        assert tokens[3].type == TokenType.STRING_LITERAL 
        assert tokens[3].value == "'data''s.sav'"

    def test_handles_subcommands(self):
        """
        Subcommands always start with forward slash.
        """
        code = "GET DATA /TYPE=TXT."
        tokens = self.lexer.tokenize(code)
        
        subcommand = tokens[2]
        assert subcommand.type == TokenType.SUBCOMMAND
        assert subcommand.value == "/TYPE"

    def test_number_vs_terminator(self):
        """
        Crucial Test: Distinguish '10.5' (Float) from '10.' (Int + Terminator)
        """
        # Case A: Float
        tokens_a = self.lexer.tokenize("10.5")
        assert len(tokens_a) == 1
        assert tokens_a[0].type == TokenType.NUMBER_LITERAL

        # Case B: Int + Terminator (e.g., COMPUTE x=10.)
        tokens_b = self.lexer.tokenize("10.")
        assert len(tokens_b) == 2
        assert tokens_b[0].type == TokenType.NUMBER_LITERAL
        assert tokens_b[0].value == "10"
        assert tokens_b[1].type == TokenType.TERMINATOR
        
    def test_identifiers_vs_functions(self):
        """
        🟢 NEW: The 'LAG' Logic Check.
        Ensures built-in functions are tokenized as FUNCTION, not IDENTIFIER.
        """
        # LAG is a function, FLAG is a variable
        code = "COMPUTE x = LAG(val) + FLAG."
        tokens = self.lexer.tokenize(code)
        
        # Tokens: [COMPUTE] [x] [=] [LAG] [(] [val] [)] [+] [FLAG] [.]
        
        # 1. Check LAG
        lag_token = tokens[3]
        assert lag_token.type == TokenType.FUNCTION, f"Expected LAG to be FUNCTION, got {lag_token.type}"
        assert lag_token.value.upper() == "LAG"

        # 2. Check FLAG (Should NOT be a function)
        flag_token = tokens[8]
        assert flag_token.type == TokenType.IDENTIFIER, f"Expected FLAG to be IDENTIFIER, got {flag_token.type}"
        assert flag_token.value == "FLAG"

    def test_complex_functions(self):
        """
        🟢 NEW: Checks RTRIM and DATE.DMY are recognized as functions.
        """
        code = "COMPUTE d = DATE.DMY(1,1,2022)."
        tokens = self.lexer.tokenize(code)
        
        # [COMPUTE] [d] [=] [DATE.DMY] ...
        func_token = tokens[3]
        
        # Assuming you added DATE.DMY to the FUNCTION regex list in grammar.py
        assert func_token.type == TokenType.FUNCTION
        assert func_token.value.upper() == "DATE.DMY"

    def test_do_if_command(self):
        """
        🟢 NEW: Verifies DO IF is treated as a single COMMAND token.
        This prevents the parser bug where 'IF' logic swallowed the line.
        """
        code = "DO IF (x = 1)."
        tokens = self.lexer.tokenize(code)
        
        # Should be [DO IF] [LPAREN] ...
        # NOT [DO] [IF] ...
        assert tokens[0].type == TokenType.COMMAND
        assert tokens[0].value.upper() == "DO IF"