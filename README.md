# SQLGlot - DB2 Dialect Support

SQLGlot is a Python library that is database-agnostic and designed for parsing, generating, and transforming SQL dialects. This project extends SQLGlot by adding support for the **DB2 dialect**, enabling seamless interaction with DB2 SQL queries.

## Installation

To use SQLGlot with DB2 dialect support, first install the SQLGlot library:

```bash
pip install sqlglot
```

Once installed, you can leverage the extended DB2 dialect features in your code.

## Usage
### Parsing
You can parse a DB2 SQL query using sqlglot.parse with the db2 dialect:


```python
import sqlglot

sql = "SELECT * FROM sysibm.systables WHERE creator = 'DB2INST1'"
parsed = sqlglot.parse(sql, dialect="db2")
print(parsed)
```
### Transpile
SQLGlot allows you to transpile SQL queries from DB2 to other dialects or vice versa:

```python
import sqlglot
sqlglot.transpile("SELECT EPOCH_MS(1618088028295)", read="duckdb", write="hive")[0]
```

### Generating
You can also generate DB2 SQL queries from Python expressions:
``` python
import sqlglot
expression = sqlglot.parse_one("SELECT * FROM my_table WHERE id = 1", dialect="db2")
generated_sql = expression.sql(dialect="db2")
```

## DB2 Dialect Support
Currently, the DB2 dialect in SQLGlot supports the following key features:

- Basic Syntax: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.

- DB2-Specific Functions: COALESCE, REGEXP_EXTRACT, STR_POSITION

- Data Types: DB2-specific data types such as VARCHAR, DECIMAL, TIMESTAMP, etc.


## Known Issues

Some complex Db2 stored procedures may require manual adjustment.

Performance tuning may be required post-migration.

## Contributing

Contributions are welcome! Please fork the repo, create a new branch, and submit a pull request.