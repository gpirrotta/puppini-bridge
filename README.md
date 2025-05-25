## Puppini Bridge

### Tool per la generazione della tabella **Puppini Bridge**, secondo lo *[Unified Star Schema](https://technicspub.com/uss/)* di [Francesco Puppini](https://www.linkedin.com/in/francescopuppini/)


> [!CAUTION]
> Codice altamente instabile, solo per test.

### Creazione della tabella di partenza (Mysql)

```console

python create_db_mysql_test.py

Esempio di utilizzo della libreria PuppiniBridgeManager con Schema Snowflake:
Creazione tabelle per lo schema Snowflake nel DB 'puppini_snowflake_mysql_test'...
Tabelle Snowflake create.
Popolamento tabelle Snowflake...
Database di test Snowflake 'puppini_snowflake_mysql_test' creato e popolato
```

![snowflake](https://github.com/user-attachments/assets/255558ea-4828-49e6-b83c-ef838720f30f)

___
### Riga di comando

E' possibile creare la Puppini Bridge sia da libreria sia da riga di comando:

**Comandi**
* **create** - crea la tabella **Puppini Bridge**
* **populate** - popola la tabella **Puppini Bridge**
* **remove-fks** - rimuove le *foreign keys* delle tabelle originali - usata solo per aiutare PowerBI ad individuare le nuove **relationships** tra le tabelle originali e la Puppini Bridge - *facoltativa*

**Parametri per la connessione**
* **--driver** - supportato solo *mysql*
* **--host** -  DB hostname, ad esempio *localhost*
* **--db-name** - nome del DB
* **--user** - username dell'utente del DB
* **--password** - password utente

**Opzioni**
* **--to-sql** - invia in console il comando SQL per la creazione o il popolamento della Puppini Bridge


### Creazione della tabella **Puppini_Bridge - comandi SQL in console**

```console
python puppini_cli.py --driver mysql --host localhost --db-name puppini_snowflake_mysql_test --user root --to-sql create

Inserisci la password per l'utente 'root' su 'localhost':

CREATE TABLE `Puppini_Bridge` (
        `Stage` VARCHAR(255) NOT NULL,
        `PBK_DimGeography` INTEGER(11),
        `PBK_DimProduct` INTEGER(11),
        `PBK_DimProductCategory` INTEGER(11),
        `PBK_DimProductDepartment` INTEGER(11),
        `PBK_DimProductSubcategory` INTEGER(11),
        `PBK_DimStore` INTEGER(11),
        `PBK_DimTime` DATE,
        `PBK_FactSales` INTEGER(11),
        `DPro_Stock` INTEGER(11),
        `DPro_UnitPrice` DECIMAL(10, 2),
        `FSal_Quantity` INTEGER(11),
        `FSal_TotalAmount` DECIMAL(12, 2)
)
;
-- Comandi per la tabella DimGeography
ALTER TABLE `DimGeography` ADD COLUMN `PBK_DimGeography` INTEGER(11);
UPDATE `DimGeography` SET `PBK_DimGeography` = `GeographyKey`;
-- Comandi per la tabella DimProduct
ALTER TABLE `DimProduct` ADD COLUMN `PBK_DimProduct` INTEGER(11);
UPDATE `DimProduct` SET `PBK_DimProduct` = `ProductKey`;
-- Comandi per la tabella DimProductCategory
ALTER TABLE `DimProductCategory` ADD COLUMN `PBK_DimProductCategory` INTEGER(11);
UPDATE `DimProductCategory` SET `PBK_DimProductCategory` = `CategoryKey`;
-- Comandi per la tabella DimProductDepartment
ALTER TABLE `DimProductDepartment` ADD COLUMN `PBK_DimProductDepartment` INTEGER(11);
UPDATE `DimProductDepartment` SET `PBK_DimProductDepartment` = `DepartmentKey`;
-- Comandi per la tabella DimProductSubcategory
ALTER TABLE `DimProductSubcategory` ADD COLUMN `PBK_DimProductSubcategory` INTEGER(11);
UPDATE `DimProductSubcategory` SET `PBK_DimProductSubcategory` = `SubcategoryKey`;
-- Comandi per la tabella DimStore
ALTER TABLE `DimStore` ADD COLUMN `PBK_DimStore` INTEGER(11);
UPDATE `DimStore` SET `PBK_DimStore` = `StoreKey`;
-- Comandi per la tabella DimTime
ALTER TABLE `DimTime` ADD COLUMN `PBK_DimTime` DATE;
UPDATE `DimTime` SET `PBK_DimTime` = `TimeKey`;
-- Comandi per la tabella FactSales
ALTER TABLE `FactSales` ADD COLUMN `PBK_FactSales` INTEGER(11);
UPDATE `FactSales` SET `PBK_FactSales` = `SalesID`;
```
___ 

### Creazione su DB (Mysql) della tabella **Puppini_Bridge**


```console
python puppini_cli.py --driver mysql --host localhost --db-name puppini_snowflake_mysql_test --user root create


Inserisci la password per l'utente 'root' su 'localhost':
Inizializzazione PuppiniBridgeManager per DB: puppini_snowflake_mysql_test su localhost...
Tentativo di connessione con: mysql://****:****@localhost:3306/puppini_snowflake_mysql_test
Connessione al database 'puppini_snowflake_mysql_test' riuscita.
Manager inizializzato con successo.

--- Azione: Creazione per 'Puppini_Bridge' (Esecuzione Diretta) ---
Processo creazione per 'Puppini_Bridge' (to_sql=False)...
  Analisi tabella sorgente (interna): DimGeography
  Analisi tabella sorgente (interna): DimProduct
  Analisi tabella sorgente (interna): DimProductCategory
  Analisi tabella sorgente (interna): DimProductDepartment
  Analisi tabella sorgente (interna): DimProductSubcategory
  Analisi tabella sorgente (interna): DimStore
  Analisi tabella sorgente (interna): DimTime
  Analisi tabella sorgente (interna): FactSales
SQL CREATE TABLE generato per 'Puppini_Bridge'.
  Generazione SQL per aggiungere colonne PBK_ alle tabelle sorgente...
Esecuzione diretta creazione 'Puppini_Bridge' e modifiche sorgenti...
  Creazione tabella 'Puppini_Bridge'...
  Tabella 'Puppini_Bridge' creata.
  Modifica tabelle sorgenti (aggiunta PBK_)...
  Modifica tabelle sorgenti completata.
Creazione e modifiche completate con successo.
Creazione di 'Puppini_Bridge' e modifica delle tabelle sorgenti eseguite con successo sul DB.
```

___ 

### Popolazione della tabella **Puppini_Bridge**

```console
python puppini_cli.py --driver mysql --host localhost --db-name puppini_snowflake_mysql_test --user root populate

python puppini_cli.py --driver mysql --host localhost --db-name puppini_snowflake_mysql_test  --user root  populate
Inserisci la password per l'utente 'root' su 'localhost':
Inizializzazione PuppiniBridgeManager per DB: puppini_snowflake_mysql_test su localhost...
Tentativo di connessione con: mysql://****:****@localhost:3306/puppini_snowflake_mysql_test
Connessione al database 'puppini_snowflake_mysql_test' riuscita.
Manager inizializzato con successo.

--- Azione: Popolamento per 'Puppini_Bridge' (Esecuzione Diretta) ---
Processo popolamento per 'Puppini_Bridge' (to_sql=False)...
  Analisi tabella sorgente (interna): DimGeography
  Analisi tabella sorgente (interna): DimProduct
  Analisi tabella sorgente (interna): DimProductCategory
  Analisi tabella sorgente (interna): DimProductDepartment
  Analisi tabella sorgente (interna): DimProductSubcategory
  Analisi tabella sorgente (interna): DimStore
  Analisi tabella sorgente (interna): DimTime
  Analisi tabella sorgente (interna): FactSales
  Preparazione dati da tabella sorgente: DimGeography
    Lette 2 righe da DimGeography.
  Preparazione dati da tabella sorgente: DimProduct
    Lette 3 righe da DimProduct.
  Preparazione dati da tabella sorgente: DimProductCategory
    Lette 3 righe da DimProductCategory.
  Preparazione dati da tabella sorgente: DimProductDepartment
    Lette 2 righe da DimProductDepartment.
  Preparazione dati da tabella sorgente: DimProductSubcategory
    Lette 3 righe da DimProductSubcategory.
  Preparazione dati da tabella sorgente: DimStore
    Lette 2 righe da DimStore.
  Preparazione dati da tabella sorgente: DimTime
    Lette 2 righe da DimTime.
  Preparazione dati da tabella sorgente: FactSales
    Lette 4 righe da FactSales.
Popolamento diretto di 'Puppini_Bridge' completato.
Popolamento di 'Puppini_Bridge' eseguito con successo sul DB.
```

![puppini_bridge](https://github.com/user-attachments/assets/ab8932c7-208e-44ae-aa53-bd49556f80f2)

### Rimozione Foreign Keys dalle tabelle originali 
(hack per aiutare PowerBI a riconoscere subito le relationships)

```console
python puppini_cli.py --driver mysql --host localhost --db-name puppini_snowflake_mysql_test --user root  remove-fks
Inserisci la password per l'utente 'root' su 'localhost':
Inizializzazione PuppiniBridgeManager per DB: puppini_snowflake_mysql_test su localhost...
Tentativo di connessione con: mysql://****:****@localhost:3306/puppini_snowflake_mysql_test
Connessione al database 'puppini_snowflake_mysql_test' riuscita.
Manager inizializzato con successo.

--- Azione: Rimozione Foreign Keys dalle tabelle sorgenti (Esecuzione Diretta) ---
Processo rimozione Foreign Keys (to_sql=False)...
Esecuzione diretta rimozione Foreign Keys...
Rimozione Foreign Keys completata con successo.
Rimozione delle Foreign Keys eseguita con successo sul DB.
```
### Risultato finale in PowerBI

![puppini-bridge-powerbi](https://github.com/user-attachments/assets/8b1aaec7-1ce7-4365-931c-4c3f902c81a9)

