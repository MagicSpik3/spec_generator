import sys
import argparse
from src.pipeline.conductor import SpecConductor

def main():
    parser = argparse.ArgumentParser(description="SPSS to Formal Spec Compiler")
    parser.add_argument("input_file", help="Path to the .sps file")
    parser.add_argument("--out", default="output", help="Output directory")
    
    args = parser.parse_args()
    
    try:
        conductor = SpecConductor()
        conductor.compile(args.input_file, args.out)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()