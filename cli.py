import argparse
from pathlib import Path
import sys
from spec_generator.importers.spss.parser import SpssParser
from spec_generator.importers.spss.graph_builder import GraphBuilder
from spec_generator.exporters.yaml import IrYamlExporter
from spec_generator.exporters.mermaid import MermaidExporter # 🟢 Import new exporter

def main():
    parser = argparse.ArgumentParser(description="SpecGen: Legacy SPSS Compiler")
    parser.add_argument("file", help="Path to input .sps file")
    # 🟢 New Flag
    parser.add_argument("--visualize", action="store_true", help="Generate a Mermaid Flowchart instead of YAML")
    
    args = parser.parse_args()
    input_path = Path(args.file)

    if not input_path.exists():
        print(f"❌ Error: File not found: {input_path}")
        sys.exit(1)

    print(f"📖 Reading {input_path.name}...")
    code = input_path.read_text(encoding="utf-8")

    print("🔍 Parsing Syntax...")
    spss_parser = SpssParser()
    try:
        nodes = spss_parser.parse(code)
        print(f"    Found {len(nodes)} commands.")
    except Exception as e:
        print(f"❌ Parse Error: {e}")
        sys.exit(1)

    print("🧠 Building Logic Graph...")
    builder = GraphBuilder()
    try:
        pipeline = builder.build(nodes)
    except Exception as e:
        print(f"❌ Build Error: {e}")
        sys.exit(1)

    # 🟢 Branch logic based on flag
    if args.visualize:
        print("🎨 Generating Visualization...")
        exporter = MermaidExporter()
        diagram = exporter.export(pipeline)
        
        output_file = input_path.with_suffix(".md")
        output_file.write_text(f"```mermaid\n{diagram}\n```", encoding="utf-8")
        print(f"✅ Diagram saved to: {output_file}")
        print("    (Preview this file in VS Code or GitHub to see the graph)")
    else:
        print("💾 Exporting YAML Artifact...")
        exporter = IrYamlExporter()
        output_file = input_path.with_suffix(".yaml")
        exporter.export(pipeline, str(output_file))
        print(f"✅ Success! Pipeline spec saved to: {output_file}")

if __name__ == "__main__":
    main()