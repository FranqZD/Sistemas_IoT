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
        print("1. ")
        print("2. ")
        print("3. ")
        print("4.")
        print("5.")
        print("6.")
        print("7.")
        print("8.")
        print("9.")
        print("10.")
        print("11.")
        print("12.")
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
            pass
        elif choice == 6:
            pass
        elif choice == 7:
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