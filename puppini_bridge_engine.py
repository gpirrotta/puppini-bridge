import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, inspect, text 
from sqlalchemy.schema import CreateTable 
from sqlalchemy.exc import SQLAlchemyError
import re
from collections import deque 

class PuppiniBridgeManager:
    def __init__(self, driver, hostname, port, database_name, username, password, silent=False): 
        """
        Inizializza il manager con i parametri di connessione al database.
        """
        self.driver = driver
        self.hostname = hostname
        self.port = str(port)
        self.database_name = database_name
        self.username = username
        self.password = password
        self.silent = silent 
        
        if self.driver != "mysql":
            if not self.silent:
                print(f"ATTENZIONE: Il driver specificato '{self.driver}' non è 'mysql'. "
                      "La libreria è ora ottimizzata per MySQL. "
                      "La compatibilità con altri driver non è garantita per tutte le funzionalità.")

        self.engine = self._create_db_engine()
        self.inspector = inspect(self.engine)
        self.metadata = MetaData() 

    def get_abbreviation(self, table_name: str) -> str: 
        """
        Genera un'abbreviazione per un nome di tabella.
        """
        parts = re.findall('([A-Z][a-z]*)', table_name)
        if not parts: 
            underscore_parts = table_name.split('_')
            if len(underscore_parts) > 1 and all(underscore_parts):
                if len(underscore_parts) == 1:
                     return underscore_parts[0][:4].capitalize()
                if len(underscore_parts) > 2: 
                    return "".join(p[0] for p in underscore_parts[:2]).upper() + \
                           "".join(p[0] for p in underscore_parts[2:4]).capitalize()
                elif len(underscore_parts) == 2: 
                    return underscore_parts[0][:2].capitalize() + underscore_parts[1][:2].capitalize()
            return table_name[:4].capitalize() if len(table_name) >=4 else table_name.upper()

        if len(parts) == 1: 
            return parts[0][:4].capitalize()
        else: 
            if len(parts) > 2 :
                 return parts[0][0].upper() + parts[1][0].upper() + parts[2][:2].capitalize()
            return parts[0][0].upper() + parts[-1][:3].capitalize()

    def _create_db_engine(self):
        """Crea e restituisce un engine SQLAlchemy."""
        try:
            connection_string = f"{self.driver}://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database_name}"
            
            if not self.silent: print(f"Tentativo di connessione con: {connection_string.replace(self.password, '****')}")
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                if not self.silent: print(f"Connessione al database '{self.database_name}' riuscita.")
            return engine
        except Exception as e:
            if not self.silent: print(f"Errore durante la creazione dell'engine SQLAlchemy: {e}")
            raise

    def _get_source_table_names(self, bridge_table_name_to_exclude="Puppini_Bridge"):
        """Recupera i nomi delle tabelle sorgente, escludendo la tabella bridge."""
        try:
            all_tables = self.inspector.get_table_names()
            return [name for name in all_tables if name.lower() != bridge_table_name_to_exclude.lower()]
        except Exception as e:
            if not self.silent: print(f"Errore nel recuperare i nomi delle tabelle: {e}")
            raise

    def _get_table_analysis_details(self, bridge_table_name_to_exclude="Puppini_Bridge"):
        """
        Analizza tutte le tabelle sorgente per PK, FK e campi numerici.
        Restituisce una mappa con i dettagli e le definizioni delle colonne per Puppini_Bridge.
        """
        source_table_names = self._get_source_table_names(bridge_table_name_to_exclude)
        
        pbk_column_defs = []
        numeric_column_defs = []
        bridge_column_names_set = {'Stage'} 
        table_details_map = {} 

        for table_name in source_table_names:
            current_details = {'pk_name': None, 'pk_type': None, 'fks': [], 'numerics': []}
            if not self.silent: print(f"  Analisi tabella sorgente (interna): {table_name}")
            
            columns_raw = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            pk_names = pk_constraint.get('constrained_columns', [])

            if len(pk_names) == 1:
                pk_col_name = pk_names[0]
                current_details['pk_name'] = pk_col_name
                pk_col_detail_obj = next((c for c in columns_raw if c['name'] == pk_col_name), None)
                if pk_col_detail_obj:
                    current_details['pk_type'] = pk_col_detail_obj['type'] 
                    bridge_pk_col_name = f"PBK_{table_name}"
                    if bridge_pk_col_name not in bridge_column_names_set:
                        pbk_column_defs.append(Column(bridge_pk_col_name, pk_col_detail_obj['type'], nullable=True))
                        bridge_column_names_set.add(bridge_pk_col_name)
            elif not self.silent and pk_names:
                 print(f"    AVVISO (interna): La tabella '{table_name}' ha PK composita o nessuna PK singola. PBK_{table_name} non aggiunta automaticamente alla bridge dalle PK.")
            elif not self.silent and not pk_names:
                 print(f"    AVVISO (interna): La tabella '{table_name}' non ha PK. PBK_{table_name} non aggiunta automaticamente alla bridge.")


            actual_fks = self.inspector.get_foreign_keys(table_name)
            source_table_fk_col_names = set()
            for fk_info in actual_fks:
                constrained_col = fk_info['constrained_columns'][0]
                referred_table = fk_info['referred_table']
                constraint_name = fk_info['name']
                source_table_fk_col_names.add(constrained_col)
                current_details['fks'].append({
                    'column_name': constrained_col, 
                    'referred_table': referred_table,
                    'constraint_name': constraint_name
                })

                bridge_referred_pk_col_name = f"PBK_{referred_table}"
                if bridge_referred_pk_col_name not in bridge_column_names_set:
                    ref_pk_constr = self.inspector.get_pk_constraint(referred_table)
                    ref_pk_names = ref_pk_constr.get('constrained_columns', [])
                    if len(ref_pk_names) == 1:
                        ref_pk_col_name = ref_pk_names[0]
                        ref_pk_col_detail_obj = next((c for c in self.inspector.get_columns(referred_table) if c['name'] == ref_pk_col_name), None)
                        if ref_pk_col_detail_obj:
                            pbk_column_defs.append(Column(bridge_referred_pk_col_name, ref_pk_col_detail_obj['type'], nullable=True))
                            bridge_column_names_set.add(bridge_referred_pk_col_name)
            
            table_abbr = self.get_abbreviation(table_name) 
            for col_raw in columns_raw:
                col_name = col_raw['name']
                col_type = col_raw['type']
                is_numeric = isinstance(col_type, (
                    sqlalchemy.types.Integer, sqlalchemy.types.SmallInteger, sqlalchemy.types.BigInteger,
                    sqlalchemy.types.Numeric, sqlalchemy.types.Float, sqlalchemy.types.DECIMAL, 
                    sqlalchemy.types.REAL))
                
                is_pk = col_name in pk_names
                is_fk = col_name in source_table_fk_col_names
                is_already_pbk_in_source = col_name.startswith("PBK_") 

                if is_numeric and not is_pk and not is_fk and not is_already_pbk_in_source: 
                    current_details['numerics'].append(col_name)
                    bridge_numeric_col_name = f"{table_abbr}_{col_name}"
                    if bridge_numeric_col_name not in bridge_column_names_set:
                        numeric_column_defs.append(Column(bridge_numeric_col_name, col_type, nullable=True))
                        bridge_column_names_set.add(bridge_numeric_col_name)
            
            table_details_map[table_name] = current_details
            
        ordered_bridge_columns = [Column('Stage', String(255), nullable=False)] + \
                                 sorted(pbk_column_defs, key=lambda c: c.name) + \
                                 sorted(numeric_column_defs, key=lambda c: c.name)
        
        return ordered_bridge_columns, bridge_column_names_set, table_details_map, source_table_names

    def create_puppini_bridge(self, bridge_table_name="Puppini_Bridge", to_sql=False):
        if not self.silent: print(f"Processo creazione per '{bridge_table_name}' (to_sql={to_sql})...")
        ordered_columns, _, table_details_map, source_tables = self._get_table_analysis_details(bridge_table_name)
        
        create_bridge_sql_str = f"-- Nessuna colonna significativa definita per {bridge_table_name}."
        if ordered_columns and len(ordered_columns) > 1: 
            local_metadata = MetaData()
            puppini_bridge_table = Table(bridge_table_name, local_metadata, *ordered_columns)
            try:
                create_bridge_sql_str = str(CreateTable(puppini_bridge_table).compile(self.engine))
                if not self.silent: print(f"SQL CREATE TABLE generato per '{bridge_table_name}'.")
            except Exception as e:
                if not self.silent: print(f"Errore durante la generazione di CREATE TABLE SQL: {e}")
                create_bridge_sql_str = f"-- ERRORE nella generazione di CREATE TABLE SQL: {e}"
        elif not self.silent:
            print(f"AVVISO: Nessuna colonna significativa (oltre a Stage) definita per '{bridge_table_name}'.")

        modify_source_sql_commands_list = []
        if not self.silent: print("  Generazione SQL per aggiungere colonne PBK_ alle tabelle sorgente...")
        for table_name in source_tables:
            details = table_details_map.get(table_name)
            if details and details.get('pk_name') and details.get('pk_type'):
                pk_col_name = details['pk_name']
                pk_col_type_obj = details['pk_type']
                try:
                    pk_col_type_sql = str(pk_col_type_obj.compile(dialect=self.engine.dialect))
                    source_pbk_col_name = f"PBK_{table_name}"
                    
                    q = "`" # Default per MySQL

                    sql_add_col = f"ALTER TABLE {q}{table_name}{q} ADD COLUMN {q}{source_pbk_col_name}{q} {pk_col_type_sql};"
                    sql_update_col = f"UPDATE {q}{table_name}{q} SET {q}{source_pbk_col_name}{q} = {q}{pk_col_name}{q};"
                    
                    modify_source_sql_commands_list.append(f"-- Comandi per la tabella {table_name}")
                    modify_source_sql_commands_list.append(sql_add_col)
                    modify_source_sql_commands_list.append(sql_update_col)
                except Exception as e_compile_type:
                     if not self.silent: print(f"    ERRORE durante la compilazione del tipo per PK di {table_name}: {e_compile_type}")

        if to_sql:
            return {
                'create_bridge_sql': create_bridge_sql_str,
                'modify_source_tables_sql': modify_source_sql_commands_list
            }
        else: 
            if not self.silent: print(f"Esecuzione diretta creazione '{bridge_table_name}' e modifiche sorgenti...")
            try:
                with self.engine.connect() as connection:
                    q_ident = "`" 
                    
                    schema_arg_for_has_table = self.database_name if self.driver == "mysql" else None

                    if sqlalchemy.inspect(self.engine).has_table(bridge_table_name, schema=schema_arg_for_has_table):
                        if not self.silent: print(f"  Tabella '{bridge_table_name}' esistente. Eliminazione...")
                        connection.execute(text(f"DROP TABLE IF EXISTS {q_ident}{bridge_table_name}{q_ident}")) 
                        if not self.silent: print(f"  Tabella '{bridge_table_name}' eliminata.")
                    
                    if not create_bridge_sql_str.startswith("--"):
                        if not self.silent: print(f"  Creazione tabella '{bridge_table_name}'...")
                        connection.execute(text(create_bridge_sql_str))
                        if not self.silent: print(f"  Tabella '{bridge_table_name}' creata.")
                    elif not self.silent:
                        print(f"  SQL per creare '{bridge_table_name}' non valido, saltato.")

                    if not self.silent: print("  Modifica tabelle sorgenti (aggiunta PBK_)...")
                    for cmd_str in modify_source_sql_commands_list:
                        if not cmd_str.startswith("--"):
                            connection.execute(text(cmd_str))
                    if not self.silent: print("  Modifica tabelle sorgenti completata.")
                    
                    connection.commit()
                if not self.silent: print("Creazione e modifiche completate con successo.")
                return True
            except Exception as e_exec:
                if not self.silent: print(f"ERRORE durante l'esecuzione diretta di create_puppini_bridge: {e_exec}")
                return False

    def populate_puppini_bridge(self, bridge_table_name="Puppini_Bridge", to_sql=False):
        if not self.silent: print(f"Processo popolamento per '{bridge_table_name}' (to_sql={to_sql})...")
        ordered_columns, bridge_cols_set, table_details_map, source_tables = self._get_table_analysis_details(bridge_table_name)

        if not ordered_columns or len(ordered_columns) <= 1:
            if not self.silent: print(f"AVVISO: Struttura di '{bridge_table_name}' non definita correttamente. Popolamento non possibile.")
            return [f"-- Struttura di {bridge_table_name} non definita correttamente."] if to_sql else False

        local_metadata = MetaData()
        puppini_bridge_table_obj = Table(bridge_table_name, local_metadata, *ordered_columns) 
        
        q = "`" 

        compiled_insert_sql_list = []

        with self.engine.connect() as connection: 
            for table_name in source_tables:
                if not self.silent: print(f"  Preparazione dati da tabella sorgente: {table_name}")
                source_table_details = table_details_map.get(table_name)
                if not source_table_details or not source_table_details.get('pk_name'):
                    if not self.silent: print(f"    Tabella {table_name} non ha PK o dettagli, saltata.")
                    continue
                
                source_table_pk_name = source_table_details['pk_name']
                result = connection.execute(sqlalchemy.text(f"SELECT * FROM {q}{table_name}{q}"))
                source_rows = result.mappings().all()
                if not self.silent: print(f"    Lette {len(source_rows)} righe da {table_name}.")

                for i, source_row_data in enumerate(source_rows):
                    insert_values_for_bridge = {'Stage': table_name}
                    current_row_pk_value = source_row_data.get(source_table_pk_name)
                    bridge_pk_col_for_source = f"PBK_{table_name}"
                    if bridge_pk_col_for_source in bridge_cols_set:
                        insert_values_for_bridge[bridge_pk_col_for_source] = current_row_pk_value

                    table_abbr = self.get_abbreviation(table_name) 
                    for numeric_col_name in source_table_details.get('numerics', []):
                        numeric_value = source_row_data.get(numeric_col_name)
                        bridge_numeric_col_name = f"{table_abbr}_{numeric_col_name}"
                        if bridge_numeric_col_name in bridge_cols_set:
                            insert_values_for_bridge[bridge_numeric_col_name] = numeric_value
                    
                    populated_pbk_cols_for_this_bridge_row = {bridge_pk_col_for_source} 
                    queue = deque()
                    for fk_info in source_table_details.get('fks', []):
                        fk_col_name_in_source = fk_info['column_name']
                        referred_table = fk_info['referred_table']
                        fk_value_in_source_row = source_row_data.get(fk_col_name_in_source)
                        bridge_col_for_fk = f"PBK_{referred_table}"
                        if bridge_col_for_fk in bridge_cols_set and bridge_col_for_fk not in populated_pbk_cols_for_this_bridge_row:
                            insert_values_for_bridge[bridge_col_for_fk] = fk_value_in_source_row
                            populated_pbk_cols_for_this_bridge_row.add(bridge_col_for_fk)
                        if fk_value_in_source_row is not None:
                             referred_table_pk_details = table_details_map.get(referred_table)
                             if referred_table_pk_details and referred_table_pk_details.get('pk_name'):
                                queue.append((referred_table, fk_value_in_source_row))
                                
                    visited_for_traversal = set() 
                    while queue:
                        current_traverse_table_name, current_traverse_pk_value = queue.popleft()
                        if (current_traverse_table_name, current_traverse_pk_value) in visited_for_traversal: continue
                        visited_for_traversal.add((current_traverse_table_name, current_traverse_pk_value))
                        traverse_table_details = table_details_map.get(current_traverse_table_name)
                        if not traverse_table_details or not traverse_table_details.get('pk_name'): continue
                        
                        stmt_text_select = f"SELECT * FROM {q}{current_traverse_table_name}{q} WHERE {q}{traverse_table_details['pk_name']}{q} = :pk_val"
                        traverse_row_result = connection.execute(text(stmt_text_select), {"pk_val": current_traverse_pk_value})
                        traverse_row_data = traverse_row_result.mappings().first() 

                        if traverse_row_data:
                            for fk_info in traverse_table_details.get('fks', []):
                                fk_col_name_in_traverse = fk_info['column_name']
                                next_referred_table = fk_info['referred_table']
                                fk_value_in_traverse_row = traverse_row_data.get(fk_col_name_in_traverse)
                                bridge_col_for_next_fk = f"PBK_{next_referred_table}"
                                if bridge_col_for_next_fk in bridge_cols_set and bridge_col_for_next_fk not in populated_pbk_cols_for_this_bridge_row:
                                    insert_values_for_bridge[bridge_col_for_next_fk] = fk_value_in_traverse_row
                                    populated_pbk_cols_for_this_bridge_row.add(bridge_col_for_next_fk)
                                if fk_value_in_traverse_row is not None:
                                    next_referred_table_pk_details = table_details_map.get(next_referred_table)
                                    if next_referred_table_pk_details and next_referred_table_pk_details.get('pk_name'):
                                        if (next_referred_table, fk_value_in_traverse_row) not in visited_for_traversal: 
                                            queue.append((next_referred_table, fk_value_in_traverse_row))
                    try:
                        stmt = puppini_bridge_table_obj.insert().values(**insert_values_for_bridge)
                        compiled_stmt_str = str(stmt.compile(self.engine, compile_kwargs={"literal_binds": True}))
                        compiled_insert_sql_list.append(compiled_stmt_str)
                        
                        if not to_sql: 
                            connection.execute(stmt) 
                    except Exception as e_pop:
                        error_msg = f"    ERRORE durante preparazione/esecuzione INSERT per riga {i+1} da {table_name}: {e_pop}"
                        if not self.silent: print(error_msg)
                        if not self.silent: print(f"      Dati: {insert_values_for_bridge}")
                        if to_sql: compiled_insert_sql_list.append(f"-- ERRORE: {error_msg}")
            
            if not to_sql:
                try:
                    connection.commit()
                    if not self.silent: print(f"Popolamento diretto di '{bridge_table_name}' completato.")
                    return True
                except Exception as e_commit:
                    if not self.silent: print(f"ERRORE durante il commit del popolamento di '{bridge_table_name}': {e_commit}")
                    return False
        
        if to_sql:
            if not self.silent: print(f"Generati {len(compiled_insert_sql_list)} comandi SQL INSERT (restituiti).")
            return compiled_insert_sql_list
        else: 
            return False


    def remove_foreign_keys(self, to_sql=False): # Aggiunto parametro to_sql
        """
        Genera o esegue i comandi SQL ALTER TABLE per rimuovere tutte le FK dalle tabelle sorgente.
        Se to_sql è True, restituisce una lista di stringhe SQL ALTER TABLE.
        Se to_sql è False, esegue gli ALTER TABLE direttamente e restituisce True/False.
        """
        if not self.silent: print(f"Processo rimozione Foreign Keys (to_sql={to_sql})...")
        source_table_names = self._get_source_table_names()
        alter_sql_commands = []

        for table_name in source_table_names:
            try:
                fks = self.inspector.get_foreign_keys(table_name)
                for fk_info in fks:
                    constraint_name = fk_info.get('name')
                    if constraint_name:
                        sql_drop_fk = ""
                        # Attualmente gestisce solo MySQL, dato che la logica per SQL Server è stata rimossa prima
                        if self.driver == "mysql":
                            q_table = "`"
                            drop_syntax = f"DROP FOREIGN KEY `{constraint_name}`"
                            sql_drop_fk = f"ALTER TABLE {q_table}{table_name}{q_table} {drop_syntax};"
                        # Se si volesse aggiungere il supporto per altri DB qui, si farebbe con elif
                        # elif self.driver == "altro_driver":
                        #    ...
                        
                        if sql_drop_fk: 
                            alter_sql_commands.append(sql_drop_fk)
            except Exception as e_get_fks:
                 if not self.silent: print(f"  ERRORE nel processare le FK per la tabella {table_name}: {e_get_fks}")
        
        if to_sql:
            if not self.silent: print(f"Generati {len(alter_sql_commands)} comandi SQL ALTER TABLE per DROP FK (restituiti).")
            return alter_sql_commands
        else: # Esegui direttamente
            if not self.silent: print("Esecuzione diretta rimozione Foreign Keys...")
            try:
                with self.engine.connect() as connection:
                    for cmd_str in alter_sql_commands:
                        if not cmd_str.startswith("--"): # Ignora commenti (es. per SQLite)
                            # if not self.silent: print(f"    Esecuzione: {cmd_str[:100]}...") # Debug
                            connection.execute(text(cmd_str))
                    connection.commit()
                if not self.silent: print("Rimozione Foreign Keys completata con successo.")
                return True
            except Exception as e_exec_drop:
                if not self.silent: print(f"ERRORE durante l'esecuzione diretta della rimozione delle FK: {e_exec_drop}")
                return False


    def analyze_naming_convention(self):
        """
        Analizza le FK nelle tabelle sorgente e restituisce un dizionario con i loro dettagli.
        Restituisce un dizionario.
        """
        if not self.silent: print("Analisi convenzione di denominazione FK...")
        source_table_names = self._get_source_table_names()
        fk_analysis = {}

        for table_name in source_table_names:
            try:
                fks = self.inspector.get_foreign_keys(table_name)
                for fk_info in fks:
                    constraint_name = fk_info.get('name')
                    constrained_columns = fk_info.get('constrained_columns')
                    referred_table = fk_info.get('referred_table')
                    referred_columns = fk_info.get('referred_columns')
                    if constraint_name and constrained_columns:
                        fk_analysis[constraint_name] = {
                            'table': table_name,
                            'columns': constrained_columns,
                            'referred_table': referred_table,
                            'referred_columns': referred_columns
                        }
            except Exception as e_analyze_fks:
                if not self.silent: print(f"  ERRORE durante l'analisi delle FK per la tabella {table_name}: {e_analyze_fks}")

        if not self.silent: print(f"Analisi convenzione FK completata. Trovati {len(fk_analysis)} vincoli FK.")
        return fk_analysis