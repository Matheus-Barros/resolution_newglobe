import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import warnings
warnings.filterwarnings("ignore")


def loading_data(logging, df_FactPupilAttendance, dim_tables):
    try:
        db_url = "sqlite:///database/newglobe.db"
        engine = create_engine(db_url)

        # Faça o upload da tabela de fatos
        logging.info('Uploading fact table "FactPupilAttendance"')
        df_FactPupilAttendance.to_sql('FactPupilAttendance', engine, if_exists="replace", index=False)

        # Faça o upload das tabelas de dimensões
        for table_name, dim_df in dim_tables.items():
            logging.info(f'Uploading dimension "{table_name}"')
            dim_df.to_sql(table_name, engine, if_exists="replace", index=False)


        logging.info('Script {script} finished with success'.format(script='loading_data.py'))
        return ('OK','df_pupilAtendance','df_pupilData')

    except Exception as e:
        logging.error('Script {script} finished with error'.format(script='loading_data.py'))
        logging.error(str(e))
        raise e
