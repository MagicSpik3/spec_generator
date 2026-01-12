import re
from src.importers.spss.tokens import TokenType

# ------------------------------------------------------------------------------
# 📜 SPSS GRAMMAR DEFINITIONS
# ------------------------------------------------------------------------------

SPSS_TOKENS = [
    # 1. Comments (Priority High)
    #    We match a star followed by anything until end of line.
    #    The Lexer will reject this match if we are not at the start of a line.
    (TokenType.COMMENT, re.compile(r"\*.*")),

    # 2. Strings
    (TokenType.STRING_LITERAL, re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", re.DOTALL)),

    # 3. Subcommands
    (TokenType.SUBCOMMAND, re.compile(r"/[A-Za-z_]+")),

    # 4. Operators & Punctuation
    (TokenType.EQUALS, re.compile(r"=")),
    (TokenType.LPAREN, re.compile(r"\(")),
    (TokenType.RPAREN, re.compile(r"\)")),
    (TokenType.COMMA, re.compile(r",")),
    (TokenType.OPERATOR, re.compile(r"(\+|-|\*|/|>=|<=|<>|<|>|&|\|)")),

    # 5. Numbers
    (TokenType.NUMBER_LITERAL, re.compile(r"\b\d+\.\d+\b|\b\.\d+\b|\b\d+\b")),

    # 6. Terminator
    (TokenType.TERMINATOR, re.compile(r"\.")),

    # 7. Identifiers
    (TokenType.IDENTIFIER, re.compile(r"[A-Za-z_@#$][A-Za-z0-9_@#$.]*")),
]