from src.importers.spss.parsers.base import BaseParserMixin
from src.importers.spss.tokens import TokenType
from src.importers.spss.ast import AggregateNode

class StatsParserMixin(BaseParserMixin):
    
    def parse_aggregate(self) -> AggregateNode:
        self.advance() # Skip AGGREGATE
        
        outfile = ""
        break_vars = []
        aggregations = []
        
        while self.current_token().type != TokenType.TERMINATOR:
            t = self.current_token()
            
            if t.type == TokenType.SUBCOMMAND:
                sub_cmd = t.value.upper()
                self.advance() 
                
                if self.current_token().type == TokenType.EQUALS:
                    self.advance()
                
                if sub_cmd == "/OUTFILE":
                    outfile = self.current_token().value.strip("'").strip('"')
                    self.advance()
                elif sub_cmd == "/BREAK":
                    while self.current_token().type == TokenType.IDENTIFIER:
                        break_vars.append(self.current_token().value)
                        self.advance()
                else:
                    # Aggregation Formula
                    target = sub_cmd.replace("/", "")
                    expr_parts = []
                    while (self.current_token().type != TokenType.SUBCOMMAND and 
                           self.current_token().type != TokenType.TERMINATOR):
                        expr_parts.append(self.current_token().value)
                        self.advance()
                    aggregations.append(f"{target} = {' '.join(expr_parts)}")
            else:
                self.advance()
                
        self.advance()
        return AggregateNode(outfile=outfile, break_vars=break_vars, aggregations=aggregations)