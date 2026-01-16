import pytest
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.ast import IgnorableNode, ComputeNode, SaveNode

def test_parser_identifies_noise_mixed_with_logic():
    """
    Scenario: A script with real logic sandwiching 'noise' commands.
    Goal: Verify the AST correctly tags TITLE and DESCRIPTIVES as IgnorableNode.
    """
    script = """
    TITLE "Starting Analysis".
    COMPUTE x = 1.
    DESCRIPTIVES VARIABLES=x.
    SAVE OUTFILE='out.sav'.
    """
    
    parser = SpssParser()
    # DEBUG: See what the Lexer thinks 'TITLE' is
    print("\n🔎 LEXER DEBUG:")
    debug_tokens = parser.lexer.tokenize(script)
    for t in debug_tokens:
        print(f"  Token: '{t.value}' \tType: {t.type}")
    print("-" * 30)
    # Note: This will fail until we define IgnorableNode and update the Parser!
    try:
        ast = parser.parse(script)
    except Exception as e:
        pytest.fail(f"Parser crashed on noise commands: {e}")

    print("\nAST Nodes Found:")
    for node in ast:
        print(f" - {type(node).__name__}: {getattr(node, 'command', 'N/A')}")

    # Assertions
    assert len(ast) == 4, f"Expected 4 nodes, found {len(ast)}"
    
    # 1. TITLE -> Ignorable
    assert isinstance(ast[0], IgnorableNode)
    assert "TITLE" in ast[0].command.upper()

    # 2. COMPUTE -> Real
    assert isinstance(ast[1], ComputeNode)

    # 3. DESCRIPTIVES -> Ignorable
    assert isinstance(ast[2], IgnorableNode)
    assert "DESCRIPTIVES" in ast[2].command.upper()

    # 4. SAVE -> Real
    assert isinstance(ast[3], SaveNode)