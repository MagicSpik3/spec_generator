from enum import Enum

class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer" 
    FLOAT = "float"
    DATE = "date"
    BOOLEAN = "boolean"
    UNKNOWN = "unknown" # Used only during transitional parsing

class OpType(str, Enum):
    LOAD = "load_csv"
    SAVE = "save_binary"
    COMPUTE = "compute_columns"
    FILTER = "filter_rows"
    JOIN = "join"
    AGGREGATE = "aggregate"
    SORT = "sort"
    # Fallback for complex SPSS commands like MATCH FILES /FILE=*
    GENERIC = "generic_transform"