import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, inspect, text # Aggiunto Date
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError
import re
from collections import deque # Per la coda nel popolamento a cascata


print("Esempio di utilizzo della libreria PuppiniBridgeManager con Schema Snowflake:")

test_db_config = {
    "driver": "mysql", 
    "hostname": "localhost",
    "port": 3306,
    "database_name": "puppini_snowflake_mysql_test", # Nuovo nome per evitare conflitti
    "username": "root",
    "password": "root" 
}

try:
    # 0. Creazione DB di test con schema Snowflake
    # Non si forza più mysql+mysqlconnector per l'admin engine
    admin_conn_str = f"mysql://{test_db_config['username']}:{test_db_config['password']}@{test_db_config['hostname']}:{test_db_config['port']}"
    
    temp_admin_engine = create_engine(admin_conn_str)
    with temp_admin_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_config['database_name']}"))
        conn.execute(text(f"CREATE DATABASE {test_db_config['database_name']}"))
        conn.commit()
    temp_admin_engine.dispose()

    # Non si forza più mysql+mysqlconnector per il test engine
    db_conn_str = f"mysql://{test_db_config['username']}:{test_db_config['password']}@{test_db_config['hostname']}:{test_db_config['port']}/{test_db_config['database_name']}"
    
    test_engine = create_engine(db_conn_str)
    with test_engine.connect() as conn:
        print(f"Creazione tabelle per lo schema Snowflake nel DB '{test_db_config['database_name']}'...")
        # Dimensioni
        # Modificato DimTime.TimeKey a DATE
        conn.execute(text("CREATE TABLE DimTime (TimeKey DATE PRIMARY KEY, FullDate DATE, Weekday VARCHAR(10)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimGeography (GeographyKey INT PRIMARY KEY, Country VARCHAR(50), Region VARCHAR(50), City VARCHAR(50)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimStore (StoreKey INT PRIMARY KEY, StoreName VARCHAR(100), GK_Ref INT, CONSTRAINT FK_Store_Geo FOREIGN KEY (GK_Ref) REFERENCES DimGeography(GeographyKey)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimProductDepartment (DepartmentKey INT PRIMARY KEY, DepartmentName VARCHAR(50)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimProductCategory (CategoryKey INT PRIMARY KEY, CategoryName VARCHAR(50), DPK_Ref INT, CONSTRAINT FK_Cat_Dept FOREIGN KEY (DPK_Ref) REFERENCES DimProductDepartment(DepartmentKey)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimProductSubcategory (SubcategoryKey INT PRIMARY KEY, SubcategoryName VARCHAR(50), CK_Ref INT, CONSTRAINT FK_Subcat_Cat FOREIGN KEY (CK_Ref) REFERENCES DimProductCategory(CategoryKey)) ENGINE=InnoDB;"))
        conn.execute(text("CREATE TABLE DimProduct (ProductKey INT PRIMARY KEY, ProductName VARCHAR(100), SCK_Ref INT, UnitPrice DECIMAL(10,2), Stock INT, CONSTRAINT FK_Prod_Subcat FOREIGN KEY (SCK_Ref) REFERENCES DimProductSubcategory(SubcategoryKey)) ENGINE=InnoDB;"))
        # Fatti
        # Modificato FactSales.TK_Ref a DATE
        conn.execute(text("""
            CREATE TABLE FactSales (
                SalesID INT PRIMARY KEY AUTO_INCREMENT, 
                TK_Ref DATE, 
                PK_Ref INT, 
                SK_Ref INT, 
                Quantity INT, 
                TotalAmount DECIMAL(12,2),
                CONSTRAINT FK_Sales_Time FOREIGN KEY (TK_Ref) REFERENCES DimTime(TimeKey),
                CONSTRAINT FK_Sales_Prod FOREIGN KEY (PK_Ref) REFERENCES DimProduct(ProductKey),
                CONSTRAINT FK_Sales_Store FOREIGN KEY (SK_Ref) REFERENCES DimStore(StoreKey)
            ) ENGINE=InnoDB;
        """))
        print("Tabelle Snowflake create.")

        print("Popolamento tabelle Snowflake...")
        # Dati per Dimensioni
        # Dati per DimTime aggiornati
        conn.execute(text("INSERT INTO DimTime VALUES ('2023-01-15', '2023-01-15', 'Sunday'), ('2023-03-20', '2023-03-20', 'Monday');"))
        conn.execute(text("INSERT INTO DimGeography VALUES (1, 'Italia', 'Lombardia', 'Milano'), (2, 'Italia', 'Lazio', 'Roma');"))
        conn.execute(text("INSERT INTO DimStore VALUES (101, 'Store Milano Duomo', 1), (102, 'Store Roma Termini', 2);"))
        conn.execute(text("INSERT INTO DimProductDepartment VALUES (1, 'Elettronica'), (2, 'Abbigliamento');"))
        conn.execute(text("INSERT INTO DimProductCategory VALUES (11, 'Computer', 1), (12, 'Smartphone', 1), (21, 'Pantaloni', 2);"))
        conn.execute(text("INSERT INTO DimProductSubcategory VALUES (111, 'Laptop', 11), (121, 'Android', 12), (211, 'Jeans', 21);"))
        conn.execute(text("INSERT INTO DimProduct VALUES (1001, 'SuperLaptop X2000', 111, 1200.00, 50), (1002, 'SmartPhone ProMax', 121, 800.00, 120), (2001, 'Jeans Blu Slim', 211, 75.00, 200);"))
        # Dati per Fatti aggiornati per TK_Ref
        conn.execute(text("INSERT INTO FactSales (TK_Ref, PK_Ref, SK_Ref, Quantity, TotalAmount) VALUES ('2023-01-15', 1001, 101, 1, 1200.00),('2023-01-15', 1002, 101, 2, 1600.00),('2023-03-20', 2001, 102, 3, 225.00),('2023-03-20', 1001, 102, 1, 1200.00);"))
        conn.commit()
    print(f"Database di test Snowflake '{test_db_config['database_name']}' creato e popolato.")
    
except():
    pass

    