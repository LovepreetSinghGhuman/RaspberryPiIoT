import logging
import json
import pyodbc
import os
from datetime import datetime

# Database configuration
SERVER = os.environ['SQL_SERVER']
DATABASE = os.environ['SQL_DATABASE']
USERNAME = os.environ['SQL_USERNAME']
PASSWORD = os.environ['SQL_PASSWORD']
DRIVER = '{ODBC Driver 17 for SQL Server}'

def get_db_connection():
    """Create database connection"""
    connection_string = f"""
        Driver={DRIVER};
        Server={SERVER};
        Database={DATABASE};
        Uid={USERNAME};
        Pwd={PASSWORD};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    """
    return pyodbc.connect(connection_string)

def init_database():
    """Initialize database table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='PresenceLogs' AND xtype='U')
        CREATE TABLE PresenceLogs (
            id INT IDENTITY(1,1) PRIMARY KEY,
            device_id NVARCHAR(100) NOT NULL,
            presence_status NVARCHAR(20) NOT NULL,
            is_present BIT NOT NULL,
            event_type NVARCHAR(50) NOT NULL,
            event_time DATETIME2 NOT NULL,
            received_time DATETIME2 DEFAULT GETUTCDATE(),
            additional_data NVARCHAR(MAX) NULL
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        # Create index for better performance
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_PresenceLogs_EventTime')
        CREATE INDEX IX_PresenceLogs_EventTime ON PresenceLogs(event_time)
        """)
        conn.commit()
        
        cursor.close()
        conn.close()
        logging.info("Database table initialized successfully")
    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        raise

def main(event: str) -> None:
    logging.info('Python EventHub trigger processing presence data')
    
    try:
        # Parse event data
        event_data = json.loads(event)
        logging.info(f"Received presence event: {event_data}")
        
        # Initialize database
        init_database()
        
        # Extract data
        device_id = event_data.get('device_id', 'Unknown')
        presence_status = event_data.get('presence_status')
        is_present = event_data.get('is_present', False)
        event_type = event_data.get('event_type', 'unknown')
        
        # Parse timestamp
        event_time_str = event_data.get('timestamp')
        if event_time_str:
            event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        else:
            event_time = datetime.utcnow()
        
        # Remove timestamp from additional data
        additional_data = event_data.copy()
        additional_data.pop('timestamp', None)
        additional_data.pop('device_id', None)
        additional_data.pop('presence_status', None)
        additional_data.pop('is_present', None)
        additional_data.pop('event_type', None)
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO PresenceLogs 
        (device_id, presence_status, is_present, event_type, event_time, additional_data)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_query, 
                      device_id,
                      presence_status,
                      is_present,
                      event_type,
                      event_time,
                      json.dumps(additional_data) if additional_data else None)
        
        conn.commit()
        
        logging.info(f"Successfully logged presence event: {presence_status} for device: {device_id}")
        
        cursor.close()
        conn.close()
        
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON: {e}")
    except KeyError as e:
        logging.error(f"Missing required field: {e}")
    except pyodbc.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")