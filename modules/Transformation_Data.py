import os
import sys
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def transformation_data(logging, df_pupilAttendance, df_pupilData):
    try:
        # Clean the data by removing leading/trailing whitespaces from all string columns
        df_pupilAttendance = df_pupilAttendance.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df_pupilData = df_pupilData.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Standardize the date columns to datetime format
        df_pupilAttendance['Date'] = pd.to_datetime(df_pupilAttendance['Date'])
        df_pupilData['SnapshotDate'] = pd.to_datetime(df_pupilData['SnapshotDate'])

        # Merge the pupil attendance and pupil data to generate the fact table
        df_FactPupilAttendance = pd.merge(df_pupilAttendance, df_pupilData, how='left', left_on=['Date','PupilID'], right_on=['SnapshotDate','PupilID'])

        # Generate unique IDs for dimensions
        academy_dict = {academy: index + 1 for index, academy in enumerate(df_FactPupilAttendance['AcademyName'].unique())}
        grade_dict = {grade: index + 1 for index, grade in enumerate(df_FactPupilAttendance['GradeName'].unique())}
        status_dict = {status: index + 1 for index, status in enumerate(df_FactPupilAttendance['Status'].unique())}
        attendance_dict = {attendance: index + 1 for index, attendance in enumerate(df_FactPupilAttendance['Attendance'].unique())}
        stream_dict = {stream: index + 1 for index, stream in enumerate(df_FactPupilAttendance['Stream'].unique())}

        # Assign the generated IDs to the fact table
        df_FactPupilAttendance['AcademyID'] = df_FactPupilAttendance['AcademyName'].map(academy_dict)
        df_FactPupilAttendance['GradeID'] = df_FactPupilAttendance['GradeName'].map(grade_dict)
        df_FactPupilAttendance['StatusID'] = df_FactPupilAttendance['Status'].map(status_dict)
        df_FactPupilAttendance['AttendanceID'] = df_FactPupilAttendance['Attendance'].map(attendance_dict)
        df_FactPupilAttendance['StreamID'] = df_FactPupilAttendance['Stream'].map(stream_dict)

        # Select the relevant columns for the fact table
        df_FactPupilAttendance = df_FactPupilAttendance[['Date', 'AcademyID', 'GradeID', 'PupilID', 'StatusID', 'AttendanceID', 'StreamID']]
        df_FactPupilAttendance[['AcademyID', 'GradeID', 'PupilID', 'StatusID', 'AttendanceID', 'StreamID']] = df_FactPupilAttendance[['AcademyID', 'GradeID', 'PupilID', 'StatusID', 'AttendanceID', 'StreamID']].astype(int)

        # Create the dimension tables
        df_dim_Pupil = df_pupilData[['PupilID', 'FirstName', 'MiddleName', 'LastName']]
        df_dim_Pupil['PupilID'] = df_dim_Pupil['PupilID'].astype(int)
        df_dim_Pupil.drop_duplicates(subset=['PupilID'], inplace=True)

        df_dim_Pupil['FirstName'] = df_dim_Pupil['FirstName'].str.title()
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.title()
        df_dim_Pupil['LastName'] = df_dim_Pupil['LastName'].str.title()

        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.replace('.', '')
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].str.replace('-', '')
        df_dim_Pupil['MiddleName'] = df_dim_Pupil['MiddleName'].replace('', pd.NA)

        df_DimAcademy = df_pupilData[['AcademyName']].drop_duplicates(subset=['AcademyName'])
        df_DimAcademy['AcademyID'] = df_DimAcademy['AcademyName'].map(academy_dict)
        df_DimAcademy['AcademyID'] = df_DimAcademy['AcademyID'].astype(int)

        df_DimGrade = df_pupilData[['GradeName']].drop_duplicates(subset=['GradeName'])
        df_DimGrade['GradeID'] = df_DimGrade['GradeName'].map(grade_dict)
        df_DimGrade['GradeID'] = df_DimGrade['GradeID'].astype(int)

        df_dim_Status = df_pupilData[['Status']].drop_duplicates(subset=['Status'])
        df_dim_Status['StatusID'] = df_dim_Status['Status'].map(status_dict)
        df_dim_Status['StatusID'] = df_dim_Status['StatusID'].astype(int)

        df_dim_attendance = df_pupilAttendance[['Attendance']].drop_duplicates(subset=['Attendance'])
        df_dim_attendance['AttendanceID'] = df_dim_attendance['Attendance'].map(attendance_dict)
        df_dim_attendance['AttendanceID'] = df_dim_attendance['AttendanceID'].astype(int)

        df_dim_stream = df_pupilData[['Stream']].drop_duplicates(subset=['Stream'])
        df_dim_stream['StreamID'] = df_dim_stream['Stream'].map(stream_dict)
        df_dim_stream['StreamID'] = df_dim_stream['StreamID'].astype(int)

        # Combine all dimension tables
        dim_tables = {
            'DimPupil': df_dim_Pupil,
            'DimAcademy': df_DimAcademy,
            'DimGrade': df_DimGrade,
            'DimStatus': df_dim_Status,
            'DimAttendance': df_dim_attendance,
            'DimStream': df_dim_stream
        }

        # Log a success message indicating the script has finished successfully
        logging.info('Script {script} finished with success'.format(script=os.path.basename(__file__)))

        # Return a tuple with the status 'OK', the fact table, and the dimension tables
        return ('OK', df_FactPupilAttendance, dim_tables)

    except Exception as e:
        # Log an error message if an exception occurs during the script execution
        _, _, tb = sys.exc_info()
        line_number = tb.tb_lineno
        logging.error('Error in the line:{line} of the script {script} ({file})'.format(line=line_number, script=os.path.basename(__file__), file=__file__))
        logging.error(str(e))

        # Raise the exception to propagate the error
        raise e
