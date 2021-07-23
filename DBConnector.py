import json
import sqlalchemy, psycopg2
import os
from carta_interview import Config, get_config_file


"""
Takes an existing DB connection, and executes queries to load values into the database.
"""
class DBConnector(object):

    __slots__ = ['conn']

    def __init__(self):
        dbcfg = None
        dbcfg_filename = get_config_file(Config.DB_CONN_JSON)

        try:
            with open(dbcfg_filename, 'r') as cfgfile:
                dbcfg = json.load(cfgfile)
        except:
            print('ERROR - Cannot read DB config file %s'.format(dbcfg_filename))
        
        # TODO dbg print('Remove me', dbcfg)

        conn = psycopg2.connect(
            dbname = dbcfg['database_name'],
            user = dbcfg['user'],
            password = dbcfg['password'],
            host=dbcfg['host'],
            port=dbcfg['port']
        )
        
        self.conn = conn
        cur = conn.cursor()

    """
    Drops all of the relevant tables (patient, encounter, period, humanname)
    """
    def execute_drop_tables(self):
        print('Dropping tables.')
        conn = self.conn
        
        cur = conn.cursor()
        
        cur.execute(self.get_sql_stmt_drop_tables())
        conn.commit()
    
    """
    Initializes all of the tables (patient, encounter, period, humanname)
    """
    def execute_init_tables(self):
        print('Creating tables')
        conn = self.conn
        cur = conn.cursor()
        
        cur.execute(self.get_sql_stmt_init_tables())
        conn.commit()
    
    """
    Inserts a HumanName entry into the database, and retrieves the row it just created as a return value
    """
    def execute_planHumanNameInsert_AndGetRow(self, tup):
        (insertedID_patient, firstname, lastname, displayname) = tup
        print('Inserting humanname entry', tup)
        conn = self.conn
        cur = conn.cursor()


        cur.execute('EXECUTE planHumanNameInsert (%s, %s, %s, %s);', (insertedID_patient, firstname, lastname, displayname))
        conn.commit()
        rowback = cur.fetchone()
        insertedID_humanname = rowback[0]
        #print(insertedID_humanname)

        return rowback

    """
    Inserts a Period entry into the database, and retrieves the row it just created as a return value
    """
    def execute_planPeriodInsert_AndGetRow(self, tup):
        (admission_dt, discharge_dt) = tup
        print('Inserting period entry', tup)
        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE planPeriodInsert (%s, %s);', (admission_dt, discharge_dt))
        conn.commit()

        rowback = cur.fetchone()
        #print('Rowback', rowback)
        insertedID_period = rowback[0]
        #print(insertedID_period)

        return rowback

    """
    Inserts a Patient entry into the database, and retrieves the row it just created as a return value
    """
    def execute_planPatientInsert_AndGetRow(self, tup):
        (mrn, birthdate) = tup
        print('Inserting patient entry', tup)
        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE planPatientInsert (%s, %s);', (mrn, birthdate))
        conn.commit()

        rowback = cur.fetchone()
        ##DBG print('Rowback', rowback)
        insertedID_patient = rowback[0]
        #DBG print(insertedID_patient)

        return rowback

    """
    Inserts a Encounter entry into the database, and retrieves the row it just created as a return value
    """
    def execute_planEncounterInsert_AndGetRow(self, tup):
        (encounterid, insertedID_patient, insertedID_period) = tup
        print('Inserting encounter entry', tup)
        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE planEncounterInsert (%s, %s, %s);', (encounterid, insertedID_patient, insertedID_period))
        conn.commit()

        rowback = cur.fetchone()
        #print(rowback)
        insertedID_encounter = rowback[0]
        #print(insertedID_encounter)
        
        return rowback

    def execute_selectHumanName_By_ID(self, tup):
        (humanname_id) = tup
        print('Selecting from humanname by humanname_id', tup)

        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE selectHumanName_By_ID (%s);', (humanname_id))
        conn.commit()

        rowback = cur.fetchone()
        print(rowback)
        
        # NOTE: Return column names, too...
        return rowback
    
    def execute_selectPeriod_By_ID(self, tup):
        (period_id) = tup
        print('Selecting from period by period_id', tup)

        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE selectPeriod_By_ID (%s);', (period_id))
        conn.commit()

        rowback = cur.fetchone()
        print(rowback)
        
        # NOTE: Return column names, too...
        return rowback

    def execute_selectPatient_By_ID(self, tup):
        (patient_id) = tup
        print('Selecting from patient by patient_id', tup)

        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE selectPatient_By_ID (%s);', (patient_id))
        conn.commit()

        rowback = cur.fetchone()
        print(rowback)
        
        # NOTE: Return column names, too...
        return rowback

    def execute_selectEncounter_By_Encounter_ID_Code(self, tup):
        (encounter_id_code) = tup
        print('Selecting from encounter by encounter_id_code', tup)

        conn = self.conn
        cur = conn.cursor()

        cur.execute('EXECUTE selectPatient_By_ID (%s);', (encounter_id_code))
        conn.commit()

        rowback = cur.fetchone()
        print(rowback)
        
        # NOTE: Return column names, too...
        return rowback
    

    def execute_define_prepared_statements(self):
        print('Defining prepared statements.')
        conn = self.conn
        cur = conn.cursor()

        cur.execute(
            "PREPARE planHumanNameInsert AS "
            
            "INSERT INTO humanname(patient_id, givenname, familyname, displayedname) VALUES ($1, $2, $3, $4) RETURNING humanname_id, patient_id, givenname, familyname, displayedname"
        
        )
        cur.execute(
                    "PREPARE planPeriodInsert AS "
                    
                    "INSERT INTO period(start_date, end_date) VALUES ($1, $2) RETURNING period_id, start_date, end_date"
                    
        )
        cur.execute(
                    "PREPARE planPatientInsert AS "
                    
                    "INSERT INTO patient(mrn_id, birthdate) VALUES ($1, $2) RETURNING patient_id, mrn_id, birthdate"
                    
        )
        cur.execute(
                    "PREPARE planEncounterInsert AS "
                    
                    "INSERT INTO encounter(encounter_id_code, patient_id, period_id) VALUES ($1, $2, $3) RETURNING encounter_id, encounter_id_code, patient_id, period_id"
                    
        )

        cur.execute(
                    "PREPARE selectHumanName_By_ID AS "
                    
                    "SELECT * FROM humanname WHERE humanname_id = ($1);"
                    
        )
        cur.execute(
                    "PREPARE selectPeriod_By_ID AS "
                    
                    "SELECT * FROM period WHERE period_id = ($1);"
                    
        )
        cur.execute(
                    "PREPARE selectPatient_By_ID AS "
                    
                    "SELECT * FROM patient WHERE patient_id = ($1);"
                    
        )
        cur.execute(
                    "PREPARE selectEncounter_By_Encounter_ID_Code AS "
                    
                    "SELECT * FROM encounter WHERE encounter_id_code = ($1);"
                    
        )
        conn.commit()

    def get_sql_stmt_drop_tables(self):
    
        sql_stmt = ''' DROP TABLE IF EXISTS encounter;
            DROP TABLE IF EXISTS humanname;
            
            DROP TABLE IF EXISTS patient;
            DROP TABLE IF EXISTS period;
        '''
        return sql_stmt
    
    def get_sql_stmt_init_tables(self):
        return """
            

            -- Table: period

            -- DROP TABLE period;

            CREATE TABLE period
            (
                period_id SERIAL,
                start_date character varying(50) COLLATE pg_catalog."default",
                end_date character varying(50) COLLATE pg_catalog."default",
                CONSTRAINT period_pkey PRIMARY KEY (period_id)
            );

            -- TABLESPACE pg_default;

            ALTER TABLE period
                OWNER to carta;
            
            -- Table: patient

            -- DROP TABLE patient;

            CREATE TABLE patient
            (
                patient_id SERIAL,
                mrn_id character varying(50) COLLATE pg_catalog."default",
                humanname_id integer,
                birthdate date,
                CONSTRAINT patient_pkey PRIMARY KEY (patient_id)
                -- CONSTRAINT patient_humanname_id_fkey FOREIGN KEY (humanname_id)
                -- REFERENCES humanname (humanname_id) MATCH SIMPLE
            );

            -- TABLESPACE pg_default;

            ALTER TABLE patient
                OWNER to carta;

            -- Table: humanname

            -- DROP TABLE humanname;

            CREATE TABLE humanname
            (
                humanname_id SERIAL,
                patient_id integer NOT NULL,
                givenname character varying(50) COLLATE pg_catalog."default",
                familyname character varying(50) COLLATE pg_catalog."default",
                displayedname character varying(100) COLLATE pg_catalog."default",
                CONSTRAINT humanname_pkey PRIMARY KEY (humanname_id),
                CONSTRAINT humanname_patient_id_fkey FOREIGN KEY (patient_id)
                    REFERENCES patient (patient_id) MATCH SIMPLE
            );

            -- TABLESPACE pg_default;

            ALTER TABLE humanname
                OWNER to carta;


            -- Table: encounter

            -- DROP TABLE encounter;

            CREATE TABLE encounter
            (
                encounter_id SERIAL,
                encounter_id_code character varying(50) NOT NULL,
                patient_id integer NOT NULL,
                period_id integer NOT NULL,
                CONSTRAINT encounter_pkey PRIMARY KEY (encounter_id),
                CONSTRAINT encounter_patient_id_fkey FOREIGN KEY (patient_id)
                    REFERENCES patient (patient_id) MATCH SIMPLE,
                CONSTRAINT encounter_period_id_fkey FOREIGN KEY (period_id)
                    REFERENCES period (period_id) MATCH SIMPLE
            );
            
            -- TABLESPACE pg_default;

            ALTER TABLE encounter
                OWNER to carta;


        """
