import os
from main import execute_query, insert_structured_record, read_all_structured_records

# Define Schema
schema = [
  {
    "table_name": "Employee",
    "file_name" : "employee.bin",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "char(20)"},
      {"name": "salary", "type": "float"}
    ]
  },
  {
    "table_name": "Dept",
    "file_name" : "dept.bin",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "char(20)"},
      {"name": "Location", "type": "varchar(40)"}
    ]
  }
]

# Clean up previous run
if os.path.exists("employee.bin"):
    os.remove("employee.bin")
if os.path.exists("dept.bin"):
    os.remove("dept.bin")

print("--- Testing Lab 02: Structured Records ---")

# Test 1: Insert Structured Record
print("\nInserting structured records...")
emp1 = {"id": 1, "name": "Alice", "salary": 5000.0}
emp2 = {"id": 2, "name": "Bob", "salary": 6000.0}
insert_structured_record("Employee", schema, emp1)
insert_structured_record("Employee", schema, emp2)
print("Inserted Alice and Bob.")

# Test 2: Read All Structured Records
print("\nReading all structured records...")
records = read_all_structured_records("Employee", schema)
for r in records:
    print(r)
assert len(records) == 2
assert records[0]['name'] == 'Alice'
assert records[1]['name'] == 'Bob'

print("\n--- Testing Lab 03: Query Processor ---")

# Test 3: INSERT Query
print("\nExecuting INSERT query...")
insert_query = "INSERT INTO Employee (id, name, salary) VALUES (3, 'Charlie', 7000.0)"
result = execute_query(insert_query, schema)
print(result)

# Test 4: SELECT Query (All fields)
print("\nExecuting SELECT * query...")
select_all = "SELECT * FROM Employee"
results = execute_query(select_all, schema)
for r in results:
    print(r)
assert len(results) == 3
assert results[2]['name'] == 'Charlie'

# Test 5: SELECT Query (Specific fields with condition)
print("\nExecuting SELECT name, salary FROM Employee WHERE id = 2...")
select_cond = "SELECT name, salary FROM Employee WHERE id = 2"
results = execute_query(select_cond, schema)
for r in results:
    print(r)
assert len(results) == 1
assert results[0]['name'] == 'Bob'
assert 'id' not in results[0]

print("\nAll tests passed successfully!")
