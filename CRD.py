import json
import threading
from sys import getsizeof
import os
import time


class DataStore:
    def __init__(self, pathto=os.getcwd()):
        # initializing the path to present working directory
        if not os.path.exists(pathto):
            os.makedirs(pathto)

        # if the path doesn't have a json file then adding a json file
        if not os.path.isfile(pathto):
            self.db_path = pathto+'/db.json'
        else:
            self.db_path = pathto
        
        # trying to read the existing file or creating new one if it doesn't exist
        try:
            file = open(self.db_path, 'r')
            filedata = json.load(file)
            self.data = filedata
            file.close()
            print('The file is read from this location' + self.db_path)
        except:
            file = open(self.db_path, 'w+')
            json.dump({},file)
            file.close()
            print('file is created in this location ' + self.db_path)
    
    def Create(self, key, value, ttl=None):
        # if the value is not a json object
        if not isinstance(value, dict):
            return False, "Incorrect data format. Only JSON object with key-value pair is acceptable."

        # if length of key > 32
        if len(key)>32:
            return False, "Key cannot be greater than 32 characters in length."
        
        # if size of json object value is greater than 16KB
        if getsizeof(value)>(16*1024):
            return False, "The value must be less than 16KB Limit."

        # if datastore exits then checking the size
        if os.path.isfile(self.db_path):
            # checking if the datastore will exceed 1GB on adding data
            if os.path.getsize(self.db_path)>((1024*1024*1024)-getsizeof(value)):
                return False, "File Size Exceeded 1GB."


        with open(self.db_path) as f:
            db = json.load(f)
        # checking if key already exists
        if key in db.keys():
            return False, "Key already exist in DataStore."       
        
        # adding ttl expiration time to json object
        temp = {}
        if ttl is not None:
            ttl = int(time.time() + abs(int(ttl)))

        temp = {
            'value': value, 
            'TTL': ttl
            }

        db[key] = temp

        # writing to file
        json.dump(db, fp = open(self.db_path,"w"), indent=2)
        return True, "Data created in DataStore."

    def Read(self, key):
        # reading the file
        with open(self.db_path) as f:
            db = json.load(f)
        
        if key not in db.keys():
            return False, "No data found for the key provided."

        ttl = db[key]['TTL']
        if not ttl:
            ttl = 0
        
        # checking if data has expired or not
        if (time.time()<ttl) or (ttl == 0):
            return True, db[key]['value']
        else:
            return False, "Requested data is expired for the key."

    def Delete(self, key):
        # reading the file
        with open(self.db_path) as f:
            db = json.load(f)
        
        # checking if key exists
        if key not in db.keys():
            return False, "No data found for the key provided."

        ttl = db[key]['TTL']
        if not ttl:
            ttl = 0

        # checking if data is expired and deleting key
        if (time.time()<ttl) or (ttl == 0):
            del db[key]
            with open(self.db_path,"w") as f:
                json.dump(db, f)
            return True, f"Data for {key} is deleted from the datastore."
        else:
            return False, "Requested data is expired for the key."