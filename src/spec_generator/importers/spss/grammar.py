import re
from spec_generator.importers.spss.tokens import TokenType

# ------------------------------------------------------------------------------
# 📜 SPSS GRAMMAR DEFINITIONS
# ------------------------------------------------------------------------------

SPSS_TOKENS = [
    # 1. Comments
    (TokenType.COMMENT, re.compile(r"\*.*")),

    # 2. Strings
    (TokenType.STRING_LITERAL, re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", re.DOTALL)),

    # 3. Explicit Commands
    (TokenType.COMMAND, re.compile(r"BEGIN\s+DATA", re.IGNORECASE)), 
    (TokenType.COMMAND, re.compile(r"END\s+DATA", re.IGNORECASE)),
    (TokenType.COMMAND, re.compile(r"SELECT\s+IF", re.IGNORECASE)),
    (TokenType.COMMAND, re.compile(r"MATCH\s+FILES", re.IGNORECASE)),
    (TokenType.COMMAND, re.compile(r"EXECUTE", re.IGNORECASE)),
    (TokenType.COMMAND, re.compile(r"AGGREGATE", re.IGNORECASE)), 
    (TokenType.COMMAND, re.compile(r"SORT(\s+CASES)?", re.IGNORECASE)), # Handles "SORT" or "SORT CASES"
    (TokenType.COMMAND, re.compile(r"DO\s+IF", re.IGNORECASE)),          # <--- MISSING
    (TokenType.COMMAND, re.compile(r"END\s+IF", re.IGNORECASE)),         # <--- MISSING
    (TokenType.COMMAND, re.compile(r"IF", re.IGNORECASE)),                # Keep "IF" after "DO IF"
    (TokenType.COMMAND, re.compile(r"COMPUTE", re.IGNORECASE)),           # <--- MISSING    # 3. Explicit Commands


    # 4. Built-in Functions (NEW BLOCK - Prevents "Variable Not Found" errors)
    # We use \b to ensure we match "LAG" but not "FLAG"
    (TokenType.FUNCTION, re.compile(r"\b(LAG|MEAN|SUM|MIN|MAX|RTRIM|LTRIM|NUMBER|STRING|SYSMIS|MISSING|MOD|TRUNC|DATE\.MDY|DATE\.DMY)\b", re.IGNORECASE)),

    
    # 4. Subcommands
    (TokenType.SUBCOMMAND, re.compile(r"/[A-Za-z_]+")),

    # 5. Operators & Punctuation
    (TokenType.EQUALS, re.compile(r"=")),
    (TokenType.LPAREN, re.compile(r"\(")),
    (TokenType.RPAREN, re.compile(r"\)")),
    (TokenType.COMMA, re.compile(r",")),
    (TokenType.OPERATOR, re.compile(r"(\+|-|\*|/|>=|<=|<>|<|>|&|\|)")),

    # 6. Numbers
    (TokenType.NUMBER_LITERAL, re.compile(r"\b\d+\.\d+\b|\b\.\d+\b|\b\d+\b")),

    # 7. Terminator
    (TokenType.TERMINATOR, re.compile(r"\.")),

    # 8. Identifiers
    (TokenType.IDENTIFIER, re.compile(r"[A-Za-z_@#$][A-Za-z0-9_@#$]*(?:\.[A-Za-z0-9_@#$]+)*")),
]