from pathlib import Path
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from spec_generator.exporters.yaml import IrYamlExporter

class SpecConductor:
    def __init__(self):
        self.parser = SpssParser()
        self.builder = GraphBuilder()
        self.exporter = IrYamlExporter()

    def compile(self, input_path: str, output_dir: str):
        """
        Reads SPSS -> Generates Spec -> Writes YAML
        """
        in_file = Path(input_path)
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📖 Reading {in_file.name}...")
        with open(in_file, "r", encoding="utf-8") as f:
            code = f.read()

        # 1. Parse Syntax
        print("🔍 Parsing Syntax...")
        ast_nodes = self.parser.parse(code)
        print(f"   Found {len(ast_nodes)} commands.")

        # 2. Build Graph (Semantics)
        print("🧠 Building Logic Graph...")
        pipeline = self.builder.build(ast_nodes)
        
        # 3. Validate
        print("🛡️ Validating Integrity...")
        pipeline.validate_integrity()
        
        # 4. Export
        out_file = out_dir / (in_file.stem + "_spec.yaml")
        print(f"💾 Writing Spec: {out_file}")
        self.exporter.export(pipeline, str(out_file))
        
        print("✅ Done!")