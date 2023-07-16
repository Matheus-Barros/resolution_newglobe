from modules import *
import logging
import warnings
import os
warnings.filterwarnings("ignore")

current_directory = os.path.dirname(os.path.abspath(__file__))

# Logging configuration
logging.basicConfig(filename=f'{current_directory}//logs//logging.log', level=logging.INFO)

def orchestrator():

    # Extraction Phase
    resp = extraction_data(logging)
    if resp[0] == 'OK':
        df_pupilAttendance = resp[1]
        df_pupilData = resp[2]
    
    # Transformation Phase
    if resp[0] == 'OK':
        resp = transformation_data(logging,df_pupilAttendance,df_pupilData)
        df_FactPupilAttendance = resp[1]
        dim_tables = resp[2]

    # Loading Phase
    if resp[0] == 'OK':
        resp = loading_data(logging,df_FactPupilAttendance,dim_tables)

    # Check the overall status and log the appropriate message
    if resp[0] == 'OK':
        logging.info('Script finished successfully')
    else:
        logging.info('Script finished with error')
        