# spec_generator
spec_generator


# SPSS Logic Specification Generator (SpecGen)

**Philosophy:** Unambiguous Specification First.
**Status:** Pre-Alpha (Architecture Design)

## 🎯 Objective
To convert legacy SPSS statistical pipelines into a **Formal, Stateful Intermediate Representation (IR)**. This IR serves as the "Ground Truth" for:
1.  **Documentation:** Human-readable Data Dictionaries and Logic specs.
2.  **Migration:** Deterministic compilation to R, Python, or SQL.
3.  **Validation:** Automated comparison of legacy vs. modern outputs.

## 🏗️ Architecture
The system follows a strict Compiler Design pattern:

1.  **Importer (Frontend):** * **Lexer:** Tokenizes raw SPSS text.
    * **Parser:** Consumes tokens to build the IR DAG (Directed Acyclic Graph).
2.  **Intermediate Representation (IR):**
    * A pure Python object model (Pydantic) representing Datasets (State) and Operations (Transitions).
    * **Strict Validation:** The IR cannot exist in an invalid state (e.g., circular dependencies, type mismatches).
3.  **Exporter (Backend):**
    * **YAML:** Serializes the IR for storage/review.
    * **R/Python:** Generates executable code from the IR.

## 🛠️ Tech Stack
* **Python 3.12+**
* **Pydantic:** For strict data modeling and validation.
* **Pytest:** For TDD-driven development.
* **PyYAML:** For IR serialization.

## 🧪 Testing Strategy
We employ **Triangulation Testing**:
* `Legacy Output (CSV)` == `Spec Runner Output (CSV)` == `Target Language Output (CSV)`

---
*Created: 2026-01-12*