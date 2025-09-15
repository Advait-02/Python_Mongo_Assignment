
# Necessary Imports

from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import warnings
warnings.filterwarnings("ignore")

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.

# Requirements
#
# Connect to MongoDB (assume default localhost)
# Create a database called "training_db"
# Create a collection called "employees"
# Insert 5 sample employee records with fields: name, email, department, salary, join_date


client = MongoClient("mongodb://localhost:27017/")


db = client["training_db"]
collection = db["employees"]


emp_to_add = (
    {"name":"Advait","email":"advait.patil@shipdelight.com","department":"IT","salary":50000,"join_date":datetime(2025, 9, 8)},
    {"name":"member_1","email":"member_1.patil@shipdelight.com","department":"HR","salary":60000,"join_date":datetime(2024, 5, 22)},
    {"name":"member_2","email":"member_2.patil@shipdelight.com","department":"Sales","salary":45000,"join_date":datetime(2024, 7, 17)},
    {"name":"member_3","email":"member_3.patil@shipdelight.com","department":"IT","salary":40000,"join_date":datetime(2025, 11, 13)},
    {"name":"member_4","email":"member_4.patil@shipdelight.com","department":"IT","salary":70000,"join_date":datetime(2023, 12, 1)}
)


collection.insert_many(emp_to_add)


# collection.delete_many({})

# # Task – 2: Implement various search and listing functionalities.
# Requirements
# List All Records: Function to display all employees
# Search by Department: Find employees in specific department
# Search by Salary Range: Find employees within salary range
# Search by Name Pattern: Find employees whose names contain specific substring
# Advanced Search: Combine multiple search criteria
# Sort Results: Sort employees by salary (ascending/descending)
# Limit Results: Implement pagination (limit and skip)

# List All Records


for i in collection.find():
    print(i)

# Search by Department


dep = input("enter department (IT,HR,Sales)")

result = collection.find({"department":dep})
for i in result:
    print(i)

# Search by Salary Range


a,b = input("enter range seprated by comma").split(",")

a = int(a)
b = int(b)

result = collection.find({
    "salary":{
        "$gte":a,
        "$lte":b
    }
})

for i in result:
    print(i)

# Search by Name Pattern


results = collection.find(
    {"name": {"$regex": "^A"}}
)

print("Employees whose name starts with A:")
for i in results:
    print(i)


# Advanced Search


query = {
    "$and": [
        {"department": "IT"},
        {"salary": {"$gte": 45000, "$lte": 70000}}
    ]
}

results = collection.find(query)
for emp in results:
    print(emp)

# Sort Results


result = collection.find({}).sort("salary",1)
for i in result:
    print(i)


result = collection.find({}).sort("salary",1)
for i in result:
    print(i)

# Limit Results


page = 2
page_size = 3

result = collection.find({}).sort("salary", -1).skip((page - 1) page_size).limit(page_size)

print(f"Page No. {page} :")

for emp in result:
    print(emp)

# Task -3
# Implement functions to add new records with validation.
# Requirements
# Single Insert: Add one employee record
# Bulk Insert: Add multiple employee records
# Input Validation: Validate data before insertion
# Duplicate Prevention: Prevent duplicate email addresses
# Auto-generated Fields: Add created_at timestamp automatically

# Single Insert: Add one employee record


employee = {
    "name": "Member_5",
    "email": "new.member@shipdelight.com",
    "department": "Finance",
    "salary": 55000,
    "join_date": datetime(2025, 9, 12),
    "created_at": datetime.utcnow()
}

def add_emp(emp: dict):
    required = ["name", "email", "department", "salary", "join_date"]
    for field in required:
        if field not in emp:
            raise ValueError(f"❌ Missing required field: {field}")

    if not isinstance(emp["salary"], int) or emp["salary"] <= 0:
        raise ValueError("❌ Salary must be a positive integer")

    if "@" not in emp["email"]:
        raise ValueError("❌ Invalid email format")

    if "created_at" not in emp:
        emp["created_at"] = datetime.utcnow()

    result = collection.insert_one(emp)
    return result.inserted_id

# Bulk Insert: Add multiple employee records


employees = [
    {
        "name": "Member_6",
        "email": "member6@shipdelight.com",
        "department": "Finance",
        "salary": 55000,
        "join_date": datetime(2025, 9, 12),
        "created_at": datetime.utcnow()
    },
    {
        "name": "Member_7",
        "email": "member7@shipdelight.com",
        "department": "IT",
        "salary": 60000,
        "join_date": datetime(2024, 5, 20),
        "created_at": datetime.utcnow()
    },
    {
        "name": "Member_8",
        "email": "member8@shipdelight.com",
        "department": "Sales",
        "salary": 48000,
        "join_date": datetime(2023, 11, 10),
        "created_at": datetime.utcnow()
    },
    {
        "name": "Member_9",
        "email": "member9@shipdelight.com",
        "department": "HR",
        "salary": 52000,
        "join_date": datetime(2022, 7, 1),
        "created_at": datetime.utcnow()
    },
    {
        "name": "Member_10",
        "email": "member10@shipdelight.com",
        "department": "Marketing",
        "salary": 65000,
        "join_date": datetime(2021, 3, 15),
        "created_at": datetime.utcnow()
    }
]


def add_emp(emp_list):
    for emp in emp_list:
        result = collection.insert_one(emp)
        print("Inserted:", result.inserted_id)

add_emp(employees)


def add_emps(emp_list):
    result = collection.insert_many(emp_list)
    return result.inserted_ids

print("Inserted IDs:", add_emps(employees))


def del_emp(emp_list):
    for emp in emp_list:
        result = collection.delete_many(emp)

del_emp(employees)

# Input Validation: Validate data before insertion


def validate_employee(emp: dict):
    required = ["name", "email", "department", "salary", "join_date"]

    for field in required:
        if field not in emp:
            raise "PLese Enter all inputs"

    if not isinstance(emp["salary"], int) or emp["salary"] <= 0:
        raise ValueError("Salary must be a positive integer")
        return True



# Duplicate Prevention: Prevent duplicate email addresses


emp1 = {
        "name": "Member_11",
        "email": "member112@shipdelight.com",
        "department": "Marketing",
        "salary": 75000,
        "join_date": datetime(2021, 3, 15),
        "created_at": datetime.utcnow()
    }

def insert_unique_employee(emp: dict):
    if collection.find_one({"email": emp["email"]}):
        print("Email already exist" )
        return None
    result = collection.insert_one(emp)
    return result.inserted_id

print(insert_unique_employee(emp1))

# Task -4
# Implement various update operations with different scenarios.
# Requirements
# Update Single Field: Update one field of an employee
# Update Multiple Fields: Update several fields at once
# Update by ID: Update employee using ObjectId
# Update by Criteria: Update multiple employees matching criteria
# Conditional Updates: Update only if certain conditions are met
# Add Modification Timestamp: Track when record was last modified

# Update Single Field: Update one field of an employee


from pymongo import ReturnDocument

result = collection.find_one_and_update(
    {"name": "Advait"},
    {"$set": {"salary": 80000}},
    return_document=ReturnDocument.AFTER
)

print(result)


# Update Multiple Fields: Update several fields at once


result = collection.find_one_and_update(
    {"name": "member_1"},
    {"$set": {"department": "Operations", "salary": 65000}},
    return_document=ReturnDocument.AFTER
)

print(result)

# Update by ID: Update employee using ObjectId


from bson import ObjectId

emp_id = input("Enter ObjectId")

result = collection.find_one_and_update(
    {"_id": ObjectId(emp_id)},
    {"$set": {"salary": 80000}},
    return_document=ReturnDocument.AFTER
)
print(result)


# Update by Criteria: Update multiple employees matching criteria


result = collection.update_many(
    {"department": "IT"},
    {"$inc": {"salary": 5000}}
)

print("Matched Records:", result.matched_count, "Modified Records:", result.modified_count)


# Conditional Updates: Update only if certain conditions are met


result = collection.update_many(
    {"salary": {"$lt": 50000}},
    {"$set": {"department": "Training"}}
)

print(result)

# Add Modification Timestamp: Track when record was last modified


from datetime import datetime

result = collection.update_one(
    {"name": "Advait"},
    {"$set": {"salary": 52000, "last_modified": datetime.utcnow()}}
)

print("Matched Records:", result.matched_count, "Modified Records:", result.modified_count)





