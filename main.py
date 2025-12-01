import connect
from Cassandra import cModel
import populate

session = connect.init_cassandra()

def print_menu():
    mm_options = {
        1: "Create Schema",
        2: "Load Data",
        3: "Cassandra",
        4: "MongoDB",
        5: "Dgraph",
        6: "Exit"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def Cassandra_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Consult by device_id and time range for sensor readings")
        print("2. Consult by sensor type for sensor readings")
        print("3. Consult by status and time range for sensor readings")
        print("4. Consult by device_id and time range for logs")
        print("5. Consult by severity level and time range for logs")
        print("6. Consult by service_name, severity level and time range for logs")
        print("7. Consult by service_name and time range for logs")
        print("8. Consult by device_id, severity level and time range for logs")
        print("9. Consult by device_id and time range for alerts")
        print("10. Consult by metric_type and time range for alerts")
        print("11. Consult by zone_id and time range for alerts")
        print("12. Consult by metric_type, value condition and time range for alerts")
        print("13. Back to Main Menu")
        choice = int(input("Enter your query choice: "))
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
            alias = input("Device alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_device_time(session, alias, start)

        elif choice == 5:
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_level(session, level, start)

        elif choice == 6:
            service = input("Service name: ")
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_service_level(session, service, level, start)

        elif choice == 7:
            service = input("Service name: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_by_service(session, service, start)

        elif choice == 8:
            alias = input("Device alias: ")
            level = input("Severity level: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_logs_count_by_device_level(session, alias, level, start)

        elif choice == 9:
            alias = input("Device alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_device_time(session, alias, start)

        elif choice == 10:
            metric = input("Metric type: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_metric_time(session, metric, start)

        elif choice == 11:
            zone = input("Zone alias: ")
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_zone_time(session, zone, start)

        elif choice == 12:
            metric = input("Metric type: ")
            value = float(input("Value: "))
            start = input("Start date (YYYY-MM-DD): ")
            cModel.get_alerts_by_metric_value_time(session, metric, value, start)

        elif choice == 13:
            break

        else:
            print("Invalid option.")

            
def Mongodb_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Register new IoT device")
        print("2. Register new user")
        print("3. Get general device information")
        print("4. View administrative events history")
        print("5. Add configuration version to device")
        print("6. Consult current or previous configurations")
        print("7. Device catalog and advanced search (type, zone, category")
        print("8. Users who manage devices by zone or type")
        print("9. Consult metadata of IoT system")
        print("10. Global text search")
        print("11. Update device state active/inactive")
        print("12. Global IoT system reports")
        print("13. Back to Main Menu")
        
        choice = int(input("Enter your query choice: "))
        
        if choice == 1:
            pass
        elif choice == 2:
            pass
        elif choice == 3:
            pass
        elif choice == 4:
            pass
        elif choice == 5:
            pass
        elif choice == 6:
            pass
        elif choice == 7:
            pass
        elif choice == 8:
            pass
        elif choice == 9:
            pass
        elif choice == 10:
            pass
        elif choice == 11:
            pass
        elif choice == 12:
            pass
        elif choice == 13:
            break
        else:
            print("Invalid option.")
            
def Dgraph_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Consult zone by name")
        print("2. Consult cluster by name")
        print("3. Consult installation by name")
        print("4. Consult IoT device by name")
        print("5. Consult devices within an installation")
        print("6. Consult all installations associated with a cluster")
        print("7. View complete hierarchy of a zone (Zone -> Cluster -> Installation -> IoT Device)")
        print("8. Consult connections between IoT devices")
        print("9. Consult total number of devices per cluster")
        print("10. Consult total number of installations within a zone")
        print("11. Consult IoT devices of a specific brand within a zone")
        print("12. Consult all devices with status 'off' within a zone")
        print("13. Back to Main Menu")


        choice = int(input("Enter your query choice: "))
        if choice == 1:
            pass
        elif choice == 2:
            pass
        elif choice == 3:
            pass
        elif choice == 4:
            pass
        elif choice == 5:
            break
        else:
            print("Invalid option.")

        
def main():
    while(True):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                pass
            if option == 2:
                print("Loading data into Cassandra from CSVs...")
                populate.populate_readings(session, "devicess.csv")
                populate.populate_logs(session, "./Cassandra/logs.csv")
                populate.populate_alerts(session, "./Cassandra/alerts.csv")
                print("Data loaded!")

            if option == 3:
                Cassandra_Queries()
            if option == 4:
                Mongodb_Queries()
            if option == 5:
                Dgraph_Queries()
            if option == 6:
                exit(0)

if __name__ == '__main__':
    main()