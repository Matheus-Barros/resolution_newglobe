import os
import sys
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import warnings
warnings.filterwarnings("ignore")

def loading_data(logging, df_FactPupilAttendance, dim_tables):
    try:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))        

        # Define the database URL for SQLite
        db_url = f"sqlite:///{parent_directory}//database//newglobe.db"
        engine = create_engine(db_url)

        # Upload the fact table "FactPupilAttendance"
        logging.info('Uploading fact table "FactPupilAttendance"')
        df_FactPupilAttendance.to_sql('FactPupilAttendance', engine, if_exists="replace", index=False)

        # Upload the dimension tables
        for table_name, dim_df in dim_tables.items():
            logging.info(f'Uploading dimension "{table_name}"')
            dim_df.to_sql(table_name, engine, if_exists="replace", index=False)

        # Log a success message indicating the script has finished successfully
        logging.info('Script {script} finished with success'.format(script=os.path.basename(__file__)))

        # Return a tuple with the status 'OK' and the names of the dataframes used
        return ('OK','')

    except Exception as e:
        # Log an error message if an exception occurs during the script execution
        _, _, tb = sys.exc_info()
        line_number = tb.tb_lineno
        logging.error('Error in the line:{line} of the script {script} ({file})'.format(line=line_number, script=os.path.basename(__file__), file=__file__))
        logging.error(str(e))
        
        # Raise the exception to propagate the error
        raise e
