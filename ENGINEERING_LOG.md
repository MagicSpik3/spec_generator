# Engineering Log

## [2026-01-12] Project Initialization: SpecGen

**Goal:** Establish a clean, TDD-led repository for the "Spec-First" migration engine.

### Context
Previous iterations suffered from "Implementation Coupling"—the logic was too tightly bound to the specific quirks of generating R strings or parsing loose regex. We are restarting with a **Formal IR** approach recommended by domain experts (Stateful DAG).

### Decisions
1.  **Strict Separation:** The `ir/` module will have zero knowledge of SPSS or R.
2.  **Pydantic for Models:** We will use Pydantic V2 for the IR to enforce strict typing and validation rules (e.g., preventing a 'Join' operation on non-existent datasets) at the model level.
3.  **Lexer/Parser Split:** We are abandoning "God Regexes" in favor of a standard tokenizer/parser pattern for reliability.

### Immediate Next Steps
1.  Define the `requirements.txt`.
2.  Write `tests/unit/test_ir_validation.py` to define the IR behavior (TDD).
3.  Implement `src/ir/model.py` to pass the tests.