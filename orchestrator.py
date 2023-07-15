from modules import *
import logging
import warnings
warnings.filterwarnings("ignore")

# Logging configuration
logging.basicConfig(filename='logs//logging.log', level=logging.INFO)

def orchestrator():

    resp = extraction_data(logging)
    if resp[0] == 'OK':
        df_pupilAtendance = resp[1]
        df_pupilData = resp[2]
    
    if resp[0] == 'OK':
        resp = transformation_data(logging,df_pupilAtendance,df_pupilData)
        df_FactPupilAttendance = resp[1]
        dim_tables = resp[2]

    if resp[0] == 'OK':
        resp = loading_data(logging,df_FactPupilAttendance,dim_tables)

    if resp[0] == 'OK':
        logging.info('Script finished successfully')
    else:
        logging.info('Script finished with error')
        