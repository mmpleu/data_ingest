import sqlite3

# Loading a custom package from functions.py
import functions as func

if __name__ == "__main__":

    # Opening SQLite connection for Python
    connection = sqlite3.connect("casestudy.db")
    
    # Creating database structure
    func.create_structure(connection)
    
    # Ingesting data from given JSON files
    func.ingest_menu(connection)
    func.ingest_reviews(connection)
    func.ingest_users(connection)
    func.ingest_outlets_from_TA(connection)
    func.ingest_outlets_from_UE(connection)
      
    # Example (or custom) query function and saving result csv file
    func.example_query(connection)
    
    # Closing SQLite connection for Python
    connection.close()

