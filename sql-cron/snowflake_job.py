import snowflake.connector
from datetime import datetime

# Set up your Snowflake connection parameters
account = 'maxdome.eu-west-1'
user = 'GAURAV_DANI'
role = 'ANALYST'
warehouse = 'BI_INTERN'
database = 'PLAYGROUND'
schema = 'HIGHTOUCH_TEST'
authenticator = 'externalbrowser'  # Okta domain for SSO



def execute_snowflake_query():
    # Establish a connection to Snowflake
    ctx = snowflake.connector.connect(
        user=user,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        authenticator=authenticator
    )

    # Create a cursor object to execute queries
    cs = ctx.cursor()

    try:
        # Execute a SQL query
        # Open and read the SQL file
        with open('delete_query.sql', 'r') as file:
            sql_query = file.read()

        print (sql_query)

        cs.execute(sql_query)

        # Fetch the result set
        one_row = cs.fetchone()

        # Print the result
        print(f"Output: {one_row[0]}")

        with open('create_query.sql', 'r') as file:
            sql_query = file.read()

        cs.execute(sql_query)

        # Fetch the result set
        one_row = cs.fetchone()

        # Print the result
        print(f"Output: {one_row[0]}")

    finally:
        # Clean up
        cs.close()
        ctx.close()
    
    try:
        # Execute a SQL query
        # Open and read the SQL file
        with open('verify_query.sql', 'r') as file:
            sql_query = file.read()

        print (sql_query)

        cs.execute(sql_query)

        # Fetch the result set
        one_row = cs.fetchone()

        # Print the result
        print(f"Output: {one_row}")
        
    finally:
        # Clean up
        cs.close()
        ctx.close()

if __name__ == "__main__":
    execute_snowflake_query()