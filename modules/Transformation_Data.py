import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def transformation_data(logging,df_pupilAtendance,df_pupilData):
    try:

        # Limpando Dados
        df_pupilAtendance = df_pupilAtendance.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df_pupilData = df_pupilData.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Padronizando Dates
        df_pupilAtendance['Date'] = pd.to_datetime(df_pupilAtendance['Date'])
        df_pupilData['SnapshotDate'] = pd.to_datetime(df_pupilData['SnapshotDate'])

        df_pupilData
        # Juntando dados para gerar os IDs
        df_FactPupilAttendance = pd.merge(df_pupilAtendance,
                                        df_pupilData,
                                        how='left',
                                        left_on=['Date','PupilID'],
                                        right_on=['SnapshotDate','PupilID'])

        # Gerando IDs únicos
        academy_dict = {academy: index + 1 for index, academy in enumerate(df_FactPupilAttendance['AcademyName'].unique())}
        grade_dict = {grade: index + 1 for index, grade in enumerate(df_FactPupilAttendance['GradeName'].unique())}
        status_dict = {status: index + 1 for index, status in enumerate(df_FactPupilAttendance['Status'].unique())}
        attendance_dict = {attendance: index + 1 for index, attendance in enumerate(df_FactPupilAttendance['Attendance'].unique())}
        stream_dict= {stream: index + 1 for index, stream in enumerate(df_FactPupilAttendance['Stream'].unique())}


        df_FactPupilAttendance['AcademyID'] = df_FactPupilAttendance['AcademyName'].map(academy_dict)
        df_FactPupilAttendance['GradeID'] = df_FactPupilAttendance['GradeName'].map(grade_dict)
        df_FactPupilAttendance['StatusID'] = df_FactPupilAttendance['Status'].map(status_dict)
        df_FactPupilAttendance['AttendanceID'] = df_FactPupilAttendance['Attendance'].map(attendance_dict)
        df_FactPupilAttendance['StreamID'] = df_FactPupilAttendance['Stream'].map(stream_dict)


        # Separando tabela fatos
        df_FactPupilAttendance = df_FactPupilAttendance[['Date','AcademyID','GradeID','PupilID','StatusID','AttendanceID','StreamID']]
        df_FactPupilAttendance[['AcademyID','GradeID','PupilID','StatusID','AttendanceID','StreamID']] = df_FactPupilAttendance[['AcademyID','GradeID','PupilID','StatusID','AttendanceID','StreamID']].astype(int)


        # Dimenssão Aunos e mantendo IDs unicos de alunos
        df_dim_Pupil = df_pupilData[['PupilID','FirstName','MiddleName','LastName']]
        df_dim_Pupil['PupilID'] = df_dim_Pupil['PupilID'].astype(int)
        df_dim_Pupil.drop_duplicates(subset=['PupilID'],inplace=True)

        df_dim_Pupil['FirstName'] = df_dim_Pupil['FirstName'].str.strip().str.title()
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.strip().str.title()
        df_dim_Pupil['LastName'] = df_dim_Pupil['LastName'].str.strip().str.title()

        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.replace('.','')
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.replace('-','')
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].replace('',pd.NA)

        def check_names(MiddleName,LastName):
            if str(MiddleName) == str(LastName):
                return pd.NA
            else:
                return MiddleName
            
        df_dim_Pupil['MiddleName'] = df_dim_Pupil.apply(lambda x : check_names(x['MiddleName'],x['LastName']),axis=1)

        # Dimenssão Escola
        df_DimAcademy = df_pupilData[['AcademyName']].drop_duplicates(subset=['AcademyName'])
        df_DimAcademy['AcademyID'] = df_DimAcademy['AcademyName'].map(academy_dict)
        df_DimAcademy['AcademyID'] = df_DimAcademy['AcademyID'].astype(int)

        # Dimenssão aulas
        df_DimGrade = df_pupilData[['GradeName']].drop_duplicates(subset=['GradeName'])
        df_DimGrade['GradeID'] = df_DimGrade['GradeName'].map(grade_dict)
        df_DimGrade['GradeID'] = df_DimGrade['GradeID'].astype(int)

        # Dimenssão Status e mantendo apenas status únicos
        df_dim_Status = df_pupilData[['Status']].drop_duplicates(subset=['Status'])
        df_dim_Status['StatusID'] = df_dim_Status['Status'].map(status_dict)
        df_dim_Status['StatusID'] = df_dim_Status['StatusID'].astype(int)

        # Dimenssão Attendance
        df_dim_attendance = df_pupilAtendance[['Attendance']].drop_duplicates(subset=['Attendance'])
        df_dim_attendance['AttendanceID'] = df_dim_attendance['Attendance'].map(attendance_dict)
        df_dim_attendance['AttendanceID'] = df_dim_attendance['AttendanceID'].astype(int)

        # Dimenssão Stream
        df_dim_stream = df_pupilData[['Stream']].drop_duplicates(subset=['Stream'])
        df_dim_stream['StreamID'] = df_dim_stream['Stream'].map(stream_dict)
        df_dim_stream['StreamID'] = df_dim_stream['StreamID'].astype(int)

        # Juntando tabelas de dimenssão
        dim_tables = {
            'DimPupil':df_dim_Pupil,
            'DimAcademy':df_DimAcademy,
            'DimGrade':df_DimGrade,
            'DimStatus':df_dim_Status,
            'DimAttendance':df_dim_attendance,
            'DimStream':df_dim_stream
        }

        logging.info('Script {script} finished with success'.format(script='transformation_data.py'))
        return ('OK',df_FactPupilAttendance,dim_tables)
    
    except Exception as e:
        logging.error('Script {script} finished with error'.format(script='transformation_data.py'))
        logging.error(str(e))
        raise e