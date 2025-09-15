# Necessary Imports
from pprint import pprint
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import warnings
warnings.filterwarnings("ignore")
from pymongo import ReturnDocument
from datetime import datetime, timezone


# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Connect to MongoDB and create a database and collection
client = MongoClient("mongodb://localhost:27017/")
db = client["training_db"]
collection = db["employees"]

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Insert 5 sample employee records with fields: name, email, department, salary, join_date</br>
emp_to_add = (
    {
        "name":"Advait",
        "email":"advait.patil@shipdelight.com",
        "department":"IT",
        "salary":50000,
        "join_date":datetime(2025, 9, 8)
    },
    {
        "name":"member_1",
        "email":"member_1.patil@shipdelight.com",
        "department":"HR",
        "salary":60000,
        "join_date":datetime(2024, 5, 22)
    },
    {
        "name":"member_2",
        "email":"member_2.patil@shipdelight.com",
        "department":"Sales",
        "salary":45000,
        "join_date":datetime(2024, 7, 17)
    },
    {
        "name":"member_3",
        "email":"member_3.patil@shipdelight.com",
        "department":"IT",
        "salary":40000,
        "join_date":datetime(2025, 11, 13)
    },
    {
        "name":"member_4",
        "email":"member_4.patil@shipdelight.com",
        "department":"IT",
        "salary":70000,
        "join_date":datetime(2023, 12, 1)
    }
)
collection.insert_many(emp_to_add)

# collection.delete_many({})

# Task – 2: Implement various search and listing functionalities.
# List All Records: Function to display all employees
for i in collection.find():
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Search by Department: Find employees in specific department
result = collection.find({"department": "IT"})
for i in result:
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Search by Salary Range: Find employees within salary range

result = collection.find({
    "salary":{
        "$gte": 50000,
        "$lte": 100000
    }
})

for i in result:
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Search by Name Pattern: Find employees whose names contain specific substring

results = collection.find(
    {"name": {"$regex": "^A"}}
)

for i in results:
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Advanced Search: Combine multiple search criteria

query = {
    "$and": [
        {"department": "IT"},
        {"salary": {"$gte": 45000, "$lte": 70000}}
    ]
}

for emp in collection.find(query):
    pprint(emp)

# Task – 2: Implement various search and listing functionalities.
# Sort Results: Sort employees by salary (ascending)

for i in collection.find({}).sort("salary",1):
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Sort Results: Sort employees by salary (descending)

for i in collection.find({}).sort("salary",-1):
    pprint(i)

# Task – 2: Implement various search and listing functionalities.
# Limit Results: Implement pagination (limit and skip)

page = 1

for emp in collection.find({}).sort("salary", -1).skip((page - 1) * 3).limit(2):
    pprint(emp)

# Task -3: Implement functions to add new records with validation.
# Single Insert: Add one employee record

employee = {
    "name": "Member_5",
    "email": "new.member@shipdelight.com",
    "department": "Finance",
    "salary": 55000,
    "join_date": datetime(2025, 9, 12),
    "created_at": datetime.now(timezone.utc)
}

def add_emp(emp):
    result = collection.insert_one(emp)
    return result

pprint(add_emp(employee))

# Task -3: Implement functions to add new records with validation.
# Bulk Insert: Add multiple employee records

employees = [
    {
        "name": "Member_6",
        "email": "member6@shipdelight.com",
        "department": "Finance",
        "salary": 55000,
        "join_date": datetime(2025, 9, 12),
        "created_at": datetime.now(timezone.utc)
    },
    {
        "name": "Member_7",
        "email": "member7@shipdelight.com",
        "department": "IT",
        "salary": 60000,
        "join_date": datetime(2024, 5, 20),
        "created_at": datetime.now(timezone.utc)
    },
    {
        "name": "Member_8",
        "email": "member8@shipdelight.com",
        "department": "Sales",
        "salary": 48000,
        "join_date": datetime(2023, 11, 10),
        "created_at": datetime.now(timezone.utc)
    },
    {
        "name": "Member_9",
        "email": "member9@shipdelight.com",
        "department": "HR",
        "salary": 52000,
        "join_date": datetime(2022, 7, 1),
        "created_at": datetime.now(timezone.utc)
    },
    {
        "name": "Member_10",
        "email": "member10@shipdelight.com",
        "department": "Marketing",
        "salary": 65000,
        "join_date": datetime(2021, 3, 15),
        "created_at": datetime.now(timezone.utc)
    }
]

def add_emp(emp_list):
    for emp in emp_list:
        result = collection.insert_one(emp)
        pprint(f"Inserted: {result.inserted_id}")

add_emp(employees)

# Task -3: Implement functions to add new records with validation.
# Input Validation: Validate data before insertion

def validate_employee(emp: dict):
    required = ["name", "email", "department", "salary", "join_date"]

    for field in required:
        if field not in emp:
            raise "PLese Enter all inputs"

    if not isinstance(emp["salary"], int) or emp["salary"] <= 0:
        raise ValueError("Salary must be a positive integer")
        return True

# Task -3: Implement functions to add new records with validation.
# Duplicate Prevention: Prevent duplicate email addresses

emp1 = {
        "name": "Member_11",
        "email": "member112@shipdelight.com",
        "department": "Marketing",
        "salary": 75000,
        "join_date": datetime(2021, 3, 15),
        "created_at": datetime.now(timezone.utc)
    }

def insert_unique_employee(emp: dict):
    if collection.find_one({"email": emp["email"]}):
        pprint("Email already exist" )
        return None
    result = collection.insert_one(emp)
    return result.inserted_id

pprint(insert_unique_employee(emp1))

## Task -4: Implement various update operations with different scenarios.
# Update Single Field: Update one field of an employee

result = collection.find_one_and_update(
    {"name": "Advait"},
    {"$set": {"salary": 80000}},
    return_document=ReturnDocument.AFTER
)

pprint(result)

## Task -4: Implement various update operations with different scenarios.
# Update Multiple Fields: Update several fields at once

result = collection.find_one_and_update(
    {"name": "member_1"},
    {"$set": {"department": "Operations", "salary": 65000}},
    return_document=ReturnDocument.AFTER
)

pprint(result)

## Task -4: Implement various update operations with different scenarios.
# Update by ID: Update employee using ObjectId


result = collection.find_one_and_update(
    {"_id": ObjectId("68c7baa081d7600dfd086957")},
    {"$set": {"salary": 80000}},
    return_document=ReturnDocument.AFTER
)
pprint(result)

## Task -4: Implement various update operations with different scenarios.
# Update by Criteria: Update multiple employees matching criteria
result = collection.update_many(
    {"department": "IT"},
    {"$inc": {"salary": 5000}}
)

print("Matched Records:", result.matched_count, "Modified Records:", result.modified_count)

## Task -4: Implement various update operations with different scenarios.
# Conditional Updates: Update only if certain conditions are met
result = collection.update_many(
    {"salary": {"$lt": 50000}},
    {"$set": {"department": "Training"}}
)

pprint(result)

## Task -4: Implement various update operations with different scenarios.
# Add Modification Timestamp: Track when record was last modifiedfrom datetime import datetime

result = collection.update_one(
    {"name": "Advait"},
    {"$set": {"salary": 52000, "last_modified": datetime.now(timezone.utc)}}
)

print("Matched Records:", result.matched_count, "Modified Records:", result.modified_count)