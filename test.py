from CRD import DataStore
import os
import time

ds1 = DataStore()
ds2 = DataStore('D:/Learn/file-based-datastore/testing')

test_data = {
    "A": {
        "data1": "value1",
        "data2": "value2",
        "data3": "value3"
    },
    "B": {
        "data1": "value1",
        "data2": "value2",
        "data3": "value3"
    },
    "C": {
        "data1": "value1",
        "data2": "value2",
        "data3": "value3",
        "data4": "value4",
    },
    "D": {
        "data1": "value1",
        "data2": "value2",
        "data3": "value3"
    }
}

# CREATION TESTS
status, message = ds1.Create('A',test_data['A'],20)
print(f"Creating A {status}, {message} ")

status, message = ds1.Create('A',test_data['A'],40)
print(f"Creating A {status}, {message} ")

status, message = ds1.Create('B',test_data['B'],300)
print(f"Creating B {status}, {message} ")

status, message = ds1.Create('C',test_data['C'])
print(f"Creating C {status}, {message} ")

status, message = ds1.Create('D',test_data['D'],200)
print(f"Creating D {status}, {message} ")

time.sleep(25)

# READING TESTS
status, message = ds1.Read('A')
print(f"READING A {status}, {message}")

status, message = ds1.Read('B')
print(f"READING B {status}, {message}")

status, message = ds1.Read('F')
print(f"READING F {status}, {message}")

# DELETION TESTS
status, message = ds1.Delete('A')
print(f"DELETING {status}A, {message}")

status, message = ds1.Delete('B')
print(f"DELETING {status}B, {message}")

status, message = ds1.Delete('F')
print(f"DELETING {status}F, {message}")



# DB2 FOR SPECIFIED PATH


# CREATION TESTS
status, message = ds2.Create('A',test_data['A'],20)
print(f"Creating A {status}, {message} ")

status, message = ds2.Create('A',test_data['A'],40)
print(f"Creating A {status}, {message} ")

status, message = ds2.Create('B',test_data['B'],300)
print(f"Creating B {status}, {message} ")

status, message = ds2.Create('C',test_data['C'])
print(f"Creating C {status}, {message} ")

status, message = ds2.Create('D',test_data['D'],200)
print(f"Creating D {status}, {message} ")

time.sleep(20)

# READING TESTS
status, message = ds2.Read('A')
print(f"READING A {status}, {message}")

status, message = ds2.Read('B')
print(f"READING B {status}, {message}")

status, message = ds2.Read('F')
print(f"READING F {status}, {message}")


# DELETION TESTS
status, message = ds2.Delete('A')
print(f"DELETING {status}A, {message}")

status, message = ds2.Delete('B')
print(f"DELETING {status}B, {message}")

status, message = ds2.Delete('F')
print(f"DELETING {status}F, {message}")