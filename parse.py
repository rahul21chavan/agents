import uuid
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from agents.utils.sas_chunker_v2 import chunk_sas_code_v3, save_chunks_to_csv
from lark import Lark, Transformer, UnexpectedInput

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

grammar = """
    ?start: block+

    ?block: macro | datastep | proc | libname | include | conditional | loop | comment

    macro: "%macro" NAME ";" block* "%mend" ";"
    datastep: "data" NAME ";" statement* "run" ";"
    proc: "proc" NAME ";" statement* "run" ";"
    libname: "libname" NAME STRING ";"
    include: "%include" STRING ";"
    conditional: "if" condition ";" statement* ";"
    loop: "do" "while" "(" condition ")" ";" statement* "end" ";"
    comment: "*" /[^;]+/ ";"

    statement: (assign_stmt | type_decl_stmt | general_stmt) ";"

    assign_stmt: NAME "=" expression
    type_decl_stmt: (("length" | "attrib" | "format") /[^;]+/)
    general_stmt: /[^;]+/

    NAME: /[a-zA-Z_][a-zA-Z0-9_]*/
    STRING: /"[^"]*"/
    condition: expression
    expression: /[^;]+/

    %import common.NUMBER
    %import common.WS
    %ignore WS
"""

parser = Lark(grammar, start="start", parser="lalr")

class SASNodeTransformer(Transformer):
    def macro(self, children):
        return {"type": "MACRO", "name": children[1], "blocks": children[2:-2]}

    def datastep(self, children):
        return {"type": "DATASTEP", "name": children[1], "statements": children[2:]}

    def proc(self, children):
        return {"type": "PROC", "name": children[1], "statements": children[2:]}

    def libname(self, children):
        return {"type": "LIBNAME", "lib": children[1], "path": children[2]}

    def include(self, children):
        return {"type": "INCLUDE", "file": children[1]}

    def conditional(self, children):
        return {"type": "IF", "condition": children[1], "statements": children[2:-1]}

    def loop(self, children):
        return {"type": "LOOP", "condition": children[2], "statements": children[4:-2]}

    def comment(self, children):
        return {"type": "COMMENT", "text": children[0].strip()}

    def assign_stmt(self, children):
        return {"type": "ASSIGN", "var": children[0], "value": children[1]}

    def type_decl_stmt(self, children):
        return {"type": "DECLARATION", "raw": str(children[0])}

    def general_stmt(self, children):
        return {"type": "STATEMENT", "raw": str(children[0])}

    def statement(self, children):
        return children[0]

    def expression(self, children):
        return children[0]

    def NAME(self, token):
        return str(token)

    def STRING(self, token):
        return str(token).strip('"')

transformer = SASNodeTransformer()

@dataclass
class ASTBlock:
    id: str
    type: str
    ast: Optional[Any]
    code: str
    error: Optional[str] = None

def parse_node(state: dict) -> dict:
    logging.info("🔍 Starting Parse Node")

    sas_code: str = state.get("sas_code", "")
    raw_chunks = chunk_sas_code_v3(sas_code)
    parsed_blocks: List[ASTBlock] = []

    for chunk in raw_chunks:
        try:
            tree = parser.parse(chunk["code"])
            ast = transformer.transform(tree)
            block = ASTBlock(
                id=chunk["id"],
                type=chunk["type"].upper(),
                ast=ast,
                code=chunk["code"]
            )
            logging.info(f"✅ Parsed block {block.id} ({block.type})")
        except UnexpectedInput as e:
            logging.error(f"❌ Failed parsing chunk {chunk['id']}: {str(e)}")
            block = ASTBlock(
                id=chunk["id"],
                type="UNKNOWN",
                ast=None,
                code=chunk["code"],
                error=str(e)
            )
        parsed_blocks.append(block)

    save_chunks_to_csv([b.__dict__ for b in parsed_blocks], "ast_blocks_latest.csv")

    return {
        **state,
        "ast_blocks": [b.__dict__ for b in parsed_blocks],
        "logs": state.get("logs", []) + [f"Parse: {len(parsed_blocks)} blocks"]
    }
