import pandas as pd
import warnings
import os
import sys
warnings.filterwarnings("ignore")

def extraction_data(logging):
    try:
        # Read the PupilAttendance.csv and PupilData.csv files using pandas
        df_pupilAttendance = pd.read_csv('data_csv//PupilAttendance.csv')
        df_pupilData = pd.read_csv('data_csv//PupilData.csv')   

        # Log a success message indicating the script has finished successfully
        logging.info('Script {script} finished with success'.format(script=os.path.basename(__file__)))

        # Return a tuple with 'OK' status and the extracted data dataframes
        return ('OK', df_pupilAttendance, df_pupilData)
    
    except Exception as e:
        # Log an error message if an exception occurs during the script execution
        _, _, tb = sys.exc_info()
        line_number = tb.tb_lineno
        logging.error('Error in the line:{line} of the script {script} ({file})'.format(line=line_number, script=os.path.basename(__file__), file=__file__))
        logging.error(str(e))
        
        # Raise the exception to propagate the error
        raise e
