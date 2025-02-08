from __future__ import annotations

import typing as t
import re

from sqlglot import exp, generator, parser, tokens, transforms


from sqlglot.dialects.dialect import (
    Dialect,
    rename_func
)


from sqlglot.parser import (
        OPTIONS_TYPE, 
        build_coalesce, 
        build_regexp_extract
    )
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from sqlglot.generator import Generator

class db2(Dialect):
    
    
    TIME_MAPPING = {
        "AM": "%p",  # Meridian indicator with or without periods
        "A.M.": "%p",  # Meridian indicator with or without periods
        "PM": "%p",  # Meridian indicator with or without periods
        "P.M.": "%p",  # Meridian indicator with or without periods
        "D": "%u",  # Day of week (1-7)
        "DAY": "%A",  # name of day
        "DD": "%d",  # day of month (1-31)
        "DDD": "%j",  # day of year (1-366)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (1-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (0-23)
        "IW": "%V",  # Calendar week of year (1-52 or 1-53), as defined by the ISO 8601 standard
        "MI": "%M",  # Minute (0-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (0-59)
        "WW": "%W",  # Week of year (1-53)
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }
    
    
    
    
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']  # Characters used to denote strings
        COMMENTS = ["--", ("/*", "*/")]  # Characters used to denote comments
        IDENTIFIERS = ["`", '"']  # DB2 uses double quotes to denote identifiers if ANSI mode is enabled
        STRING_ESCAPES = ["'", '"', "\\"]  # Characters used for escaping in strings
        
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ACTIVATE": TokenType.COMMAND,
            "ALIAS": TokenType.ALIAS,
            "COMMENT": TokenType.COMMENT,
            "DATABASE": TokenType.DATABASE,
            "DECLARE": TokenType.DECLARE,
            "DEFAULT": TokenType.DEFAULT,
            "ESCAPE": TokenType.ESCAPE,
            "EXCEPT": TokenType.EXCEPT,
            "EXECUTE": TokenType.EXECUTE,
            "FINAL": TokenType.FINAL,
            "FUNCTION": TokenType.FUNCTION,
            "GLOBAL": TokenType.GLOBAL,
            "GRANT": TokenType.GRANT,
            "HASH": TokenType.HASH,
            "HINT": TokenType.HINT,
            "IN": TokenType.IN,
            "INDEX": TokenType.INDEX,
            "INNER": TokenType.INNER,
            "INSERT": TokenType.INSERT,
            "JOIN": TokenType.JOIN,
            "KEY": TokenType.KEY,
            "LANGUAGE": TokenType.LANGUAGE,
            "LATERAL": TokenType.LATERAL,
            "LEFT": TokenType.LEFT,
            "LIKE": TokenType.LIKE,
            "LOCK": TokenType.LOCK,
            "NATURAL": TokenType.NATURAL,
            "NCHAR": TokenType.NCHAR,
            "NOT": TokenType.NOT,
            "NULL": TokenType.NULL,
            "ON": TokenType.ON,
            "OPTION": TokenType.OPTION,
            "OR": TokenType.OR,
            "OUTER": TokenType.OUTER,
            "PARAMETER": TokenType.PARAMETER,
            "PARTITION": TokenType.PARTITION,
            "PROCEDURE": TokenType.PROCEDURE,
            "RANGE": TokenType.RANGE,
            "RECURSIVE": TokenType.RECURSIVE,
            "REFERENCES": TokenType.REFERENCES,
            "REFRESH": TokenType.REFRESH,
            "RENAME": TokenType.RENAME,
            "RIGHT": TokenType.RIGHT,
            "ROLLBACK": TokenType.ROLLBACK,
            "ROLLUP": TokenType.ROLLUP,
            "ROW": TokenType.ROW,
            "ROWS": TokenType.ROWS,
            "SCHEMA": TokenType.SCHEMA,
            "SELECT": TokenType.SELECT,
            "SEQUENCE": TokenType.SEQUENCE,
            "SET": TokenType.SET,
            "SOME": TokenType.SOME,
            "SOURCE": TokenType.SOURCE,
            "TABLE": TokenType.TABLE,
            "TEMPORARY": TokenType.TEMPORARY,
            "THEN": TokenType.THEN,
            "UNION": TokenType.UNION,
            "UNIQUE": TokenType.UNIQUE,
            "UPDATE": TokenType.UPDATE,
            "USING": TokenType.USING,
            "VALUES": TokenType.VALUES,
            "VARIANT": TokenType.VARIANT,
            "VIEW": TokenType.VIEW,
            "WHEN": TokenType.WHEN,
            "WHERE": TokenType.WHERE,
            "WITH": TokenType.WITH,
            "YEAR": TokenType.YEAR
        }
        
        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.REPLACE} - {TokenType.SHOW}
        
    class Parser(parser.Parser):
        
        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
        WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}
        VALUES_FOLLOWED_BY_PAREN = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "COALESCE": lambda args: build_coalesce(args, is_nvl=True),  # NVL in Oracle is similar to COALESCE in DB2
            # "TIMESTAMP_FORMAT": build_formatted_time(exp.StrToTime, "db2"),  # TO_TIMESTAMP in Oracle to TIMESTAMP_FORMAT in DB2
            # "TO_DATE": lambda args: build_formatted_time(exp.StrToDate, "db2"),  # Use TO_DATE directly as DB2 also supports it
            "REGEXP_EXTRACT": lambda args: build_regexp_extract(args, is_regexp_extract=True),
            # "LEFT": lambda  args: build_left(args, is_left=True),
            # "RIGHT": lambda  args: build_right(args, is_right=True),
            "STR_POSITION": lambda args: exp.StrPosition(this=seq_get(args, 0), substr=seq_get(args, 1))
        }
        
        
    class Generator(generator.Generator):
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # exp.RegexpExtract: rename_func("REGEXP_SUBSTR")
            # exp.RegexpExtract: lambda self, e: self.func(
            #     "REGEXP_SUBSTR",
            #     e.this,
            #     e.expression,
            #     e.args.get("position"),
            #     e.args.get("occurrence"),
            # )
        }
        
        UNSIGNED_TYPE_MAPPING = {
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UINT: "INTEGER",
            exp.DataType.Type.UMEDIUMINT: "INTEGER",  
            exp.DataType.Type.USMALLINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",   
            exp.DataType.Type.UDECIMAL: "DECIMAL",
        }

        TIMESTAMP_TYPE_MAPPING = {
            exp.DataType.Type.DATETIME2: "TIMESTAMP",
            exp.DataType.Type.SMALLDATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP WITH TIME ZONE",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP WITH TIME ZONE",
        }

        
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **UNSIGNED_TYPE_MAPPING,
            **TIMESTAMP_TYPE_MAPPING,
        }
        
        
        def regexp_extract_sql(self, expression: exp.RegexpExtract2) -> str:
            func_name = "REGEXP_EXTRACT" if expression.args.get("is_regexp_extract") else "REGEXP_SUBSTR"
            return rename_func(func_name)(self, expression)