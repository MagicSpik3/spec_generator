import pytest
from src.importers.spss.lexer import SpssLexer
from src.importers.spss.tokens import TokenType

class TestSpssLexer:
    
    def setup_method(self):
        self.lexer = SpssLexer()

    def test_basic_command_tokenization(self):
        """
        Verifies simple command splitting.
        """
        code = "COMPUTE x = 1."
        tokens = self.lexer.tokenize(code)
        
        assert len(tokens) == 5
        assert tokens[0].type == TokenType.IDENTIFIER # COMPUTE (Parser promotes to CMD)
        assert tokens[0].value == "COMPUTE"
        assert tokens[1].type == TokenType.IDENTIFIER # x
        assert tokens[2].type == TokenType.EQUALS     # =
        assert tokens[3].type == TokenType.NUMBER_LITERAL # 1
        assert tokens[4].type == TokenType.TERMINATOR # .

    def test_handles_quoted_strings(self):
        """
        SPSS allows both single and double quotes and escaped quotes.
        """
        # Test escaped single quote: 'data''s.sav' -> "data's.sav"
        code = "SAVE OUTFILE='data''s.sav'."
        tokens = self.lexer.tokenize(code)
        
        # [SAVE] [OUTFILE] [=] ['data''s.sav'] [.]
        assert tokens[2].type == TokenType.EQUALS        # Index 2 is '='
        assert tokens[3].type == TokenType.STRING_LITERAL # Index 3 is the string
        assert tokens[3].value == "'data''s.sav'"

    def test_handles_subcommands(self):
        """
        Subcommands always start with forward slash.
        """
        code = "GET DATA /TYPE=TXT."
        tokens = self.lexer.tokenize(code)
        
        # GET (ID) DATA (ID) /TYPE (SUB) = (EQ) TXT (ID) . (TERM)
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
        
    def test_identifiers_with_dots(self):
        """
        SPSS variable names can contain dots, e.g., 'DATE.DMY'.
        The lexer must not break this into [DATE] [.] [DMY].
        """
        code = "COMPUTE new_date = DATE.DMY(1,1,2024)."
        tokens = self.lexer.tokenize(code)
        
        # DATE.DMY should be ONE token
        # [COMPUTE] [new_date] [=] [DATE.DMY] [(] ...
        func_token = tokens[3]
        assert func_token.type == TokenType.IDENTIFIER
        assert func_token.value == "DATE.DMY"