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
            
def Mongodb_Queries():
    while True:
        print("\n--- QUERIES ---")
        print("1. Register new IoT device")
        print("2. Register new user")
        print("3. Get general device information")
        print("4. View administrative events history")
        print("5. Add configuration version to device")
        print("6. Consult current or previous configurations")
        print("7. Generate general catalog of devices")
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
        print("1. ")
        print("2. ")
        print("3. ")
        print("4. ")
        print("5. ")
        print("6. ")
        print("7. ")
        print("8. ")
        print("9. ")
        print("10. ")
        print("11. ")
        print("12. ")
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
                pass
            if option == 3:
                Cassandra_Queries()
            if option == 4:
                Mongodb_Queries()
            if option == 5:
                Dgraph_Queries()
            if option == 6:
                exit(0)