import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def extraction_data(logging):
    try:
        df_pupilAtendance = pd.read_csv('data_csv//PupilAttendance.csv')
        df_pupilData = pd.read_csv('data_csv//PupilData.csv')   

        logging.info('Script {script} finished with success'.format(script='extraction_data.py'))
        return ('OK',df_pupilAtendance,df_pupilData)
    
    except Exception as e:
        logging.error('Script {script} finished with error'.format(script='extraction_data.py'))
        logging.error(str(e))
        raise e