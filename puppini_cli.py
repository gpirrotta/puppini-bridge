import argparse
import getpass
import json # Per stampare il dizionario di analyze-fks in modo leggibile
from puppini_bridge_engine import PuppiniBridgeManager # Assumendo che la libreria sia in puppini_bridge_library.py

def main():
    parser = argparse.ArgumentParser(description="CLI per PuppiniBridgeManager per generare o eseguire SQL.")
    
    # Argomenti di connessione al DB e opzione --to-sql globale
    parser.add_argument("--driver", required=True, choices=['mysql'], help="Driver del database (solo 'mysql' supportato)")
    parser.add_argument("--host", required=True, help="Hostname del server DB")
    parser.add_argument("--port", type=int, help="Porta del server DB (default: 3306 per mysql)")
    parser.add_argument("--db-name", required=True, help="Nome del database")
    parser.add_argument("--user", required=True, help="Username per la connessione al DB")
    parser.add_argument("--password", help="Password per la connessione al DB (verrà chiesta se non fornita)")
    parser.add_argument("--bridge-name", default="Puppini_Bridge", help="Nome della tabella Puppini Bridge (default: Puppini_Bridge)")
    parser.add_argument("--to-sql", action="store_true", help="Se presente, stampa l'SQL generato invece di eseguirlo. Applicabile a create, populate, remove-fks.")

    # Sotto-comandi per le azioni
    subparsers = parser.add_subparsers(dest="action", title="Azioni", required=True,
                                       help="Azione da eseguire con PuppiniBridgeManager")

    create_parser = subparsers.add_parser("create", help="Genera SQL o esegue la creazione della tabella Puppini Bridge e modifica le tabelle sorgenti.")
    populate_parser = subparsers.add_parser("populate", help="Genera SQL INSERT o esegue il popolamento della tabella Puppini Bridge.")
    removefks_parser = subparsers.add_parser("remove-fks", help="Genera SQL o esegue la rimozione delle Foreign Keys dalle tabelle sorgenti.") # Descrizione aggiornata
    analyzefks_parser = subparsers.add_parser("analyze-fks", help="Analizza e restituisce le Foreign Keys presenti nelle tabelle sorgenti.")

    args = parser.parse_args()

    if args.port is None:
        if args.driver == "mysql":
            args.port = 3306
        # Logica per porta di SQL Server rimossa

    db_password_to_use = args.password 
    if args.password is None:
        db_password_to_use = getpass.getpass(f"Inserisci la password per l'utente '{args.user}' su '{args.host}': ")
        if not db_password_to_use: 
            if not args.to_sql: 
                print("ATTENZIONE: È stata inserita una password vuota. Si tenterà la connessione senza password.")
            db_password_to_use = "" 
    
    try:
        manager_silent = args.to_sql

        if not manager_silent: 
            print(f"Inizializzazione PuppiniBridgeManager per DB: {args.db_name} su {args.host}...")
        
        manager = PuppiniBridgeManager(
            driver=args.driver,
            hostname=args.host,
            port=args.port,
            database_name=args.db_name,
            username=args.user,
            password=db_password_to_use, 
            silent=manager_silent 
        )
        
        if not manager_silent:
            print("Manager inizializzato con successo.")

        if args.action == "create":
            if not args.to_sql: 
                print(f"\n--- Azione: Creazione per '{args.bridge_name}' (Esecuzione Diretta) ---")
            # else: # Non stampare l'intestazione se to_sql è True, per output SQL pulito
                # print(f"\n--- SQL per CREATE {args.bridge_name} (to_sql=True) ---") # Rimosso per output pulito
            
            creation_result = manager.create_puppini_bridge(bridge_table_name=args.bridge_name, to_sql=args.to_sql)
            
            if args.to_sql:
                print(creation_result['create_bridge_sql'].strip())
                if not creation_result['create_bridge_sql'].strip().startswith("--") and not creation_result['create_bridge_sql'].strip().endswith(";"): print(";")

                if creation_result['modify_source_tables_sql']:
                    # print("\n--- SQL per MODIFICARE TABELLE SORGENTE (Aggiunta PBK_) (to_sql=True) ---") # Rimosso per output pulito
                    for cmd in creation_result['modify_source_tables_sql']:
                        print(cmd.strip())
                        if not cmd.strip().startswith("--") and not cmd.strip().endswith(";"): print(";")
            else: 
                if not manager_silent: 
                    if creation_result: 
                        print(f"Creazione di '{args.bridge_name}' e modifica delle tabelle sorgenti eseguite con successo sul DB.")
                    else:
                        print(f"ERRORE durante l'esecuzione diretta della creazione di '{args.bridge_name}' o modifica sorgenti.")
        
        elif args.action == "populate":
            if not args.to_sql: 
                print(f"\n--- Azione: Popolamento per '{args.bridge_name}' (Esecuzione Diretta) ---")
            # else: 
                # print(f"\n--- SQL per POPULATE {args.bridge_name} (to_sql=True) ---") # Rimosso per output pulito

            result = manager.populate_puppini_bridge(bridge_table_name=args.bridge_name, to_sql=args.to_sql)

            if args.to_sql:
                sql_inserts = result
                if sql_inserts and not (len(sql_inserts) == 1 and sql_inserts[0].startswith("-- ")):
                    for sql_cmd in sql_inserts:
                        print(sql_cmd.strip()) 
                        if not sql_cmd.strip().startswith("--") and not sql_cmd.strip().endswith(";"): print(";")
            else: 
                if not manager_silent:
                    if result: 
                        print(f"Popolamento di '{args.bridge_name}' eseguito con successo sul DB.")
                    else:
                        print(f"ERRORE durante l'esecuzione diretta del popolamento di '{args.bridge_name}'.")


        elif args.action == "remove-fks":
            if not args.to_sql: 
                 print("\n--- Azione: Rimozione Foreign Keys dalle tabelle sorgenti (Esecuzione Diretta) ---")
            # else: 
                 # print(f"\n--- SQL per REMOVE Foreign Keys (to_sql=True) ---") # Rimosso per output pulito
            
            result = manager.remove_foreign_keys(to_sql=args.to_sql)

            if args.to_sql:
                sql_remove_fks_list = result
                if sql_remove_fks_list:
                    for sql_cmd in sql_remove_fks_list:
                        print(sql_cmd.strip())
                        if not sql_cmd.strip().startswith("--") and not sql_cmd.strip().endswith(";"): print(";")
            else: # Esecuzione diretta
                if not manager_silent:
                    if result:
                        print("Rimozione delle Foreign Keys eseguita con successo sul DB.")
                    else:
                        print("ERRORE durante l'esecuzione diretta della rimozione delle Foreign Keys.")


        elif args.action == "analyze-fks":
            # Questo metodo della libreria restituisce sempre dati di analisi.
            # Il flag to_sql nella CLI controlla solo la verbosità della CLI stessa.
            if not args.to_sql:
                print("\n--- Azione: Analisi Foreign Keys ---")
            # else: # Non serve un'intestazione specifica per to_sql qui, stamperà solo JSON
                # print(f"\n--- Analisi Foreign Keys (to_sql=True) ---")
                
            fk_analysis = manager.analyze_naming_convention()
            if fk_analysis:
                print(json.dumps(fk_analysis, indent=2)) 
            elif not args.to_sql: # Stampa solo se non siamo in modalità solo SQL e non c'è output
                print("-- Nessuna Foreign Key trovata o analizzata.")
            
            if not args.to_sql: # Stampa solo se non siamo in modalità solo SQL
                 print("\nNOTA: L'azione 'analyze-fks' mostra sempre l'analisi; non esegue modifiche al DB.")


    except Exception as e:
        # Stampa sempre gli errori critici, anche se to_sql è True, perché indicano un problema
        print(f"\nERRORE DURANTE L'ESECUZIONE DELLA CLI: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
