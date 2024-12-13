localize_db = """Please look through the following Stack Overflow problem description and provide a list of tables that are relevant to fix the problem.

### Stack Overflow Description ###
{problem_statement}

###

### Database Structure ###
{structure}

###

Please only provide the full name of the tables and return as a Python list of strings.
The returned list should be wrapped with ```Python and ```.
For example:
```Python
["table1", "table2"]
```
"""
"I am trying to count bonds by their bond types. My query is\n```sql\nSELECT COUNT(b.bond_id) AS total,\n       b1.bond AS bond,\n       b2.bond_type_dash AS bond_type_dash,\n       b3.bond_type_equal AS bond_type_equal,\n       b4.bond_type_hash AS bond_type_hash\nFROM bond b\nLEFT JOIN (\n    SELECT bond_id, COUNT(*) AS bond\n    FROM bond\n    WHERE bond_id = 'TR000_1_2'\n) AS b1 ON b1.bond_id = b.bond_id\nLEFT JOIN (\n    SELECT bond_id, COUNT(*) AS bond_type_dash\n    FROM bond\n    WHERE bond_type = '-'\n) AS b2 ON b2.bond_id = b.bond_id\nLEFT JOIN (\n    SELECT bond_id, COUNT(*) AS bond_type_equal\n    FROM bond\n    WHERE bond_type = '='\n) AS b3 ON b3.bond_id = b.bond_id\nLEFT JOIN (\n    SELECT bond_id, COUNT(*) AS bond_type_hash\n    FROM bond\n    WHERE bond_type = '#'\n) AS b4 ON b4.bond_id = b.bond_id;\n``` but it gives me error\n```error\n1140, \"In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'toxicology.bond.bond_id'; this is incompatible with sql_mode=only_full_group_by\"```\n. How can I fix it?""
