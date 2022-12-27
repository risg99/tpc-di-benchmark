# %%
# importing libraries
import os
import re
import psycopg2
import psycopg2.extras as extras
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# %% [markdown]
# ### Populate Tables

# %%
# set up connection variables
db_host = "localhost"
db_port = "5432"
db_user = "postgres"
db_pass = "abcd"
db_name = "tpc_di"

# function to connect with postgres
def connect_postgres(db_host, db_port, db_user, db_pass, db_name):
    try:
        # Connect to an existing database
        connection = psycopg2.connect(host = db_host,
                                      port = db_port,
                                      user = db_user,
                                      password = db_pass,
                                      database = db_name)
        # Set auto-commit
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        # Create a cursor to perform database operations
        cur = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cur.execute("SELECT version();")
        # Fetch result
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    else:
        return cur

# %%
# connect to postgres

cur = connect_postgres(db_host, db_port, db_user, db_pass, db_name)

# %%
# drop tables if exists in db
cur.execute(
    """
    
    TRUNCATE TABLE staging.audit;
    TRUNCATE TABLE staging.batchdate;
    TRUNCATE TABLE staging.cashtransaction;
    TRUNCATE TABLE staging.customermgmt;
    TRUNCATE TABLE staging.dailymarket;
    TRUNCATE TABLE staging.date;
    TRUNCATE TABLE staging.finwire;
    TRUNCATE TABLE staging.finwire_cmp;
    TRUNCATE TABLE staging.finwire_fin;
    TRUNCATE TABLE staging.finwire_sec;
    TRUNCATE TABLE staging.holdinghistory;
    TRUNCATE TABLE staging.hr;
    TRUNCATE TABLE staging.industry;
    TRUNCATE TABLE staging.prospect;
    TRUNCATE TABLE staging.statustype;
    TRUNCATE TABLE staging.taxrate;
    TRUNCATE TABLE staging.time;
    TRUNCATE TABLE staging.trade;
    TRUNCATE TABLE staging.tradehistory;
    TRUNCATE TABLE staging.tradetype;
    TRUNCATE TABLE staging.watchhistory;
    
    
    """
)
print("SQL Status Output:\n", cur.statusmessage)

# %%
cur.execute(open("staging_data_commands.sql", "r").read())
print("Staging DB tables populated")

# %%
sql_commands_file = open('staging_finwire_load1.sql','w')

path = os.getcwd() + '\\sf_3\\output_data\\Batch1'

files = [f for f in os.listdir(path) if not f.startswith('.')]

for file in files:
    abs_path = path + '/'+ file
    if (re.search('^FINWIRE', file)) and (re.search('audit.csv$', file) == None):
        sql_command = "COPY staging.finwire"+" FROM '"+abs_path+"';\n"
        sql_commands_file.write(sql_command)

sql_commands_file.close()

# %%
cur.execute(open("staging_finwire_load1.sql", "r").read())
print("Finwire DB tables populated")

# %%
cur.execute(open("load_staging_finwire_db.sql", "r").read())
print("Finwire CMP, SEC and FIN Staging DB tables populated")

# %%
cur.close()

