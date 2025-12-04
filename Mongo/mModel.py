import json

def create_mongo_indexes(db):
    print("Creating MongoDB indexes...")

    db.devices.create_index("device_id", unique=True)
    db.devices.create_index("type")
    db.devices.create_index("category")
    db.devices.create_index("location.zone")
    db.devices.create_index("is_active")
    db.devices.create_index([("type", 1), ("location.zone", 1)])
    db.devices.create_index([
        ("tags", "text"),
        ("model", "text"),
        ("type", "text"),
        ("location.zone", "text")
    ])

    db.users.create_index("email", unique=True)
    db.users.create_index("user_id", unique=True)

    db.metadata.create_index("manufacturer")
    db.metadata.create_index("supported_models")
    db.metadata.create_index([("supported_models", "text")])

    print("Indexes created.\n")


# Devices

def register_device(db):
    print("\n--- Register New Device ---")
    data = {
        "device_id": input("Device ID: "),
        "device_alias": input("Alias: "),
        "type": input("Type: "),
        "model": input("Model: "),
        "location": {"zone": input("Location (zone): ")},
        "status": input("Status (ON/OFF): "),
        "tags": input("Tags (comma separated): ").split(","),
        "settings_history": [],
        "admin_events": []
    }
    db.devices.insert_one(data)
    print("✓ Device registered.\n")

def get_general_device_info(db):
    dev = input("Device ID: ")
    d = db.devices.find_one({"device_id": dev}, {"_id": 0})
    if not d:
        print("Not found.")
        return
    print(json.dumps(d, indent=2))

def view_admin_events(db):
    dev = input("Device ID: ")
    d = db.devices.find_one({"device_id": dev}, {"admin_events": 1, "_id": 0})
    print(d or "Not found.")

def add_configuration_version(db):
    dev = input("Device ID: ")
    version = input("Version description: ")
    entry = {"version_info": version}
    db.devices.update_one({"device_id": dev}, {"$push": {"settings_history": entry}})
    print("✓ Version added.\n")

def get_configurations(db):
    dev = input("Device ID: ")
    d = db.devices.find_one({"device_id": dev}, {"settings_history": 1, "_id": 0})
    if not d:
        print("Not found.")
        return
    print(json.dumps(d, indent=2))

def advanced_device_search(db):
    type_ = input("Type (blank = ignore): ")
    zone = input("Zone (blank = ignore): ")
    category = input("Category (blank = ignore): ")

    q = {}
    if type_:
        q["type"] = type_
    if zone:
        q["location.zone"] = zone
    if category:
        q["category"] = category  # from metadata

    for d in db.devices.find(q, {"_id": 0}):
        print(json.dumps(d, indent=2))

def update_device_state(db):
    dev = input("Device ID: ")
    new_state = input("New state (ON/OFF): ")
    db.devices.update_one({"device_id": dev}, {"$set": {"status": new_state}})
    print("✓ Updated.\n")
    


# Users

def register_user(db):
    user = {
        "user_id": input("User ID: "),
        "name": input("Name: "),
        "email": input("Email: "),
        "role": input("Role: "),
        "manages_zone": input("Manages zone: "),
        "manages_type": input("Manages device type: "),
    }
    db.users.insert_one(user)
    print("User added.\n")

def users_by_zone_or_type(db):
    zone = input("Zone (blank = ignore): ")
    type_ = input("Type (blank = ignore): ")

    q = {}
    if zone:
        q["manages_zone"] = zone
    if type_:
        q["manages_type"] = type_

    for u in db.users.find(q, {"_id": 0}):
        print(json.dumps(u, indent=2))

# Metadata 
def view_metadata(db):
    for m in db.metadata.find({}, {"_id": 0}):
        print(json.dumps(m, indent=2))

def global_text_search(db):
    q = input("Search text: ")
    for d in db.devices.find({"$text": {"$search": q}}, {"_id": 0}):
        print(json.dumps(d, indent=2))

def system_report(db):
    pipeline = [
        {"$group": {"_id": "$location", "count": {"$sum": 1}}}
    ]
    for row in db.devices.aggregate(pipeline):
        print(json.dumps(row, indent=2))

def devices_by_category(db):
    print("\n--- Devices Count by Category ---")

    pipeline = [
        {
            "$lookup": {
                "from": "metadata",
                "localField": "model",
                "foreignField": "supported_models",
                "as": "meta"
            }
        },
        {"$unwind": "$meta"},
        {"$unwind": "$meta.categories"},
        {
            "$group": {
                "_id": "$meta.categories",
                "total_devices": {"$sum": 1}
            }
        },
        {"$sort": {"total_devices": -1}}
    ]

    for doc in db.devices.aggregate(pipeline):
        print(json.dumps(doc, indent=2))



def active_inactive_summary(db):
    print("\n--- Active vs Inactive Devices ---")

    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1}
            }
        }
    ]

    for doc in db.devices.aggregate(pipeline):
        print(json.dumps(doc, indent=2))
