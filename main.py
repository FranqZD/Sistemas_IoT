import connect
from Cassandra import cModel
import populate
import populateD
import random_data_generator

from connect import init_mongo
from Mongo import mModel
from Dgraph import dModel

session = connect.init_cassandra()

db = None
clientD = connect.init_dgraph()

def print_menu():
    mm_options = {
        1: "Create Schema",
        2: "Load Data",
        3: "Devices",
        4: "Logs",
        5: "Alerts",
        6: "Users",
        7: "Metadata",
        8: "Infrastructure",
        9: "Exit"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def Devices_Queries():
    global db
    if db is None:
        db = init_mongo()
        
    while True:
        print("\n--- QUERIES ---")
        print("0. Show available device IDs")
        print("1. Consult by device_alias and time range for devices")
        print("2. Consult by sensor type for devices")
        print("3. Consult by status and time range for devices")
        print("4. Register new IoT device")
        print("5. Get general device information")
        print("6. View administrative events history")
        print("7. Add configuration version to device")
        print("8. Consult current or previous configurations")
        print("9. Device catalog and advanced search (type, zone, category")
        print("10. Update device state active/inactive")
        print("11. Find installation by device name")
        print("12. List offline devices in a cluster")
        print("13. Show connections for a specific device")
        print("14. Count IoT devices in a cluster")
        print("15. Get brand name by device name")
        print("16. List devices by brand name")
        print("17. Exit")

        choice = int(input("Enter your query choice: "))
        if choice == 0:
            mModel.list_device_ids(db)
            
        if choice == 1:
            alias = input("Device alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_readings_by_device(session, alias, start)

        elif choice == 2:
            sensor_type = input("Sensor type: ")
            cModel.get_devices_by_type(session, sensor_type)

        elif choice == 3:
            status = input("Status: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_devices_by_status_time(session, status, start)
            
        elif choice == 4:
            mModel.register_device(db)

        elif choice == 5:
            mModel.get_general_device_info(db)

        elif choice == 6:
            mModel.view_admin_events(db)

        elif choice == 7:
            mModel.add_configuration_version(db)

        elif choice == 8:
            mModel.get_configurations(db)

        elif choice == 9:
            mModel.advanced_device_search(db)

        elif choice == 10:
            mModel.update_device_state(db)
        
        elif choice == 11:
            dModel.get_place_of_device(connect.init_dgraph(), input("Device name: "))
            
        elif choice == 12:
            dModel.get_inactive_devices_in_cluster(connect.init_dgraph(), input("Cluster name: "))
            
        elif choice == 13:
            dModel.get_device_connections(connect.init_dgraph(), input("Device name: "))
        elif choice == 14:
            dModel.count_devices_in_cluster(connect.init_dgraph(), input("Cluster name: "))
        elif choice == 15:
            dModel.get_brand_by_device(connect.init_dgraph(), input("Device name: "))
        elif choice == 16:
            dModel.get_devices_by_brand(connect.init_dgraph(), input("Brand name:"))
        elif choice == 17:
            break
        else:
            print("")

    
def Logs_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Consult by device_alias and time range for logs")
        print("2. Consult by severity level and time range for logs")
        print("3. Consult by service_name, severity level and time range for logs")
        print("4. Consult by service_name and time range for logs")
        print("5. Consult by device_alias, severity level and time range for logs")
        print("6. Back to Main Menu")
        
        choice = int(input("Enter your query choice: "))
        
        if choice == 1:
            alias = input("Device alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_device_time(session, alias, start)

        elif choice == 2:
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_level(session, level, start)

        elif choice == 3:
            service = input("Service name: ")
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_service_level(session, service, level, start)

        elif choice == 4:
            service = input("Service name: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_service(session, service, start)

        elif choice == 5:
            alias = input("Device alias: ")
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_count_by_device_level(session, alias, level, start)

        elif choice == 6:
            break
        else:
            print("Invalid option.")
            
def Alerts_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Consult by device_alias and time range for alerts")
        print("2. Consult by metric_type and time range for alerts")
        print("3. Consult by zone_alias and time range for alerts")
        print("4. Consult by metric_type, value condition and time range for alerts")
        print("5. Back to Main Menu")

        choice = int(input("Enter your query choice: "))

        if choice == 1:
            alias = input("Device alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_device_time(session, alias, start)

        elif choice == 2:
            metric = input("Metric type: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_metric_time(session, metric, start)

        elif choice == 3:
            zone = input("Zone alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_zone_time(session, zone, start)

        elif choice == 4:
            metric = input("Metric type: ")
            value = float(input("Value: "))
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_metric_value_time(session, metric, value, start)

        elif choice == 5:
            break
        else:
            print("Invalid option.")


def Users_Queries():
    global db
    if db is None:
        db = init_mongo()

    while True:
        print("\n--- QUERIES ---")
        print("0. Show avaiable user IDs")
        print("1. Register new user")
        print("2. Users who manage devices by zone or type")
        print("3. Back to Main Menu")
        
        choice = int(input("Enter your query choice: "))
        
        if choice == 0:
            mModel.list_user_ids(db)
        
        if choice == 1:
            mModel.register_user(db)

        elif choice == 2:
            mModel.users_by_zone_or_type(db)

        elif choice == 3:
            break
        else:
            print("Invalid option.")

def Metadata_Queries():
    global db
    if db is None:
        db = init_mongo()

    while True:
        print("\n--- QUERIES ---")
        print("1. Consult metadata of IoT system")
        print("2. Global text search")
        print("3. Global IoT system reports")
        print("4. Devices by category")
        print("5. Active vs inactive summary")
        print("6. Back to Main Menu")
        
        choice = int(input("Enter your query choice: "))
        
        if choice == 1:
            mModel.view_metadata(db)

        elif choice == 2:
            mModel.global_text_search(db)

        elif choice == 3:
            mModel.system_report(db)

        elif choice == 4:
            mModel.devices_by_category(db)

        elif choice == 5:
            mModel.active_inactive_summary(db)

        elif choice == 6:
            break

        else:
            print("Invalid option.")

def Infrastructure_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. List all installations in a cluster")
        print("2. Find installations in a specific zone")
        print("3. Filter clusters in a zone by status")
        print("4. List all clusters in a zone")
        print("5. Show installations of a specific type in a cluster")
        
        choice = int(input("Enter your query choice: "))
        
        if choice == 1:
            dModel.get_places_by_cluster(clientD, input("Cluster name: "))
        elif choice == 2:
            dModel.get_places_related_to_zone(clientD, input("Zone name: "))
        elif choice == 3:
            dModel.get_clusters_by_status_in_zone(clientD, input("Zone name: "), input("Status: "))
        elif choice == 4:
            dModel.get_all_clusters_in_zone(clientD, input("Zone name: "))
        elif choice == 5:
            dModel.get_places_by_type_in_cluster(clientD, input("Cluster name: "), input("Place type: "))
        elif choice == 6:
            break
        else:
            print("Invalid option.")

        
def main():
    global db
    
    while(True):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                dModel.create_Schema(clientD)
                pass
            if option == 2:
                print("\nGenerating random CSVs...")
                random_data_generator.generate_all_csvs()

                print("Loading data into Cassandra from CSVs...")
                populate.populate_readings(session, "devices.csv")
                populate.populate_logs(session, "./Cassandra/logs.csv")
                populate.populate_alerts(session, "./Cassandra/alerts.csv")

                print("Loading data into Dgraph from CSVs...")
                populateD.load_data(clientD)

                print("Loading data into MongoDB from CSVs...")
                db = init_mongo()
                populate.load_mongo(db)

                print("Data loaded!")


            if option == 3:
                Devices_Queries()
            if option == 4:
                Logs_Queries()
            if option == 5:
                Alerts_Queries()
            if option == 6:
                Users_Queries()
            if option == 7:
                Metadata_Queries()
            if option == 8:
                Infrastructure_Queries()
            if option == 9:
                exit(0)

if __name__ == '__main__':
    main()