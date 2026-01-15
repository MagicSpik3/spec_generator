import pytest
from spec_generator.importers.spss.lexer import SpssLexer
from spec_generator.importers.spss.tokens import TokenType

class TestLexerRobustness:
    """
    Integration-style tests for the Lexer.
    Checks how it handles 'Real World' vs 'Garbage' input.
    """
    
    def setup_method(self):
        self.lexer = SpssLexer()

    def test_parses_real_world_messy_code(self):
        """
        Scenario: Real code is rarely clean. It has extra spaces, 
        mixed case, and inline comments.
        """
        raw_code = """
        * This is a header comment.
        GET   DATA 
           /TYPE = TXT
           /FILE = 'C:\\data\\my file.txt'.
           
        COMPUTE   AgeDays = (DATE.DMY(1,1,2024) - DOB) / (60*60*24).
        """
        
        tokens = self.lexer.tokenize(raw_code)
        
        # We expect a stream of valid tokens, filtering out the comment
        # 1. GET DATA
        assert tokens[0].value == "GET"
        assert tokens[1].value == "DATA"
        # 2. /TYPE = TXT
        assert tokens[2].type == TokenType.SUBCOMMAND
        assert tokens[2].value == "/TYPE"
        
        # Scan ahead to check the calculation
        # Find the EQUALS token for COMPUTE
        compute_eq_index = -1
        for i, t in enumerate(tokens):
            if t.value == "AgeDays":
                compute_eq_index = i + 1
                break
        
        assert tokens[compute_eq_index].type == TokenType.EQUALS

    def test_garbage_input_raises_error(self):
        """
        Scenario: Random binary or unsupported characters should trigger
        a clear SyntaxError, not a crash or silent failure.
        """
        # The backtick ` is not in our grammar
        garbage = "COMPUTE x = `broken`."
        
        with pytest.raises(SyntaxError, match="Unexpected character '`'"):
            self.lexer.tokenize(garbage)

    def test_incomplete_code_is_tokenized(self):
        """
        Scenario: A file ends abruptly (no terminator).
        The lexer should process what it can. The PARSER will handle the error later.
        """
        incomplete = "COMPUTE x ="
        tokens = self.lexer.tokenize(incomplete)
        
        assert len(tokens) == 3
        assert tokens[2].type == TokenType.EQUALS