# NewGlobe Data Engineering ETL Process

This repository contains the ETL (Extract, Transform, Load) process for NewGlobe Data Engineering team to prepare a data mart for analyzing pupil data in academies. The ETL process extracts data from CSV files, transforms and cleans the data, and loads it into an SQLite database for further analysis.

The ETL process is divided into three main steps:

1. **Extraction**: The data is extracted from CSV files into Pandas DataFrames.
2. **Transformation**: The extracted data is transformed and cleaned, generating a fact table and dimension tables.
3. **Loading**: The transformed data is loaded into an SQLite database.

## Project Structure

The project structure is organized as follows:

```
- data_csv/               # Directory containing the CSV data files
    - PupilAttendance.csv
    - PupilData.csv
- database/               # Directory containing the SQLite database
    - newglobe.db
- logs/                   # Directory containing log files
    - logging.log
- modules/                # Directory containing the ETL modules
    - Extraction_Data.py
    - Transformation_Data.py
    - Loading_Data.py
- main.py                 # Script to initiate the ETL process
- orchestrator.py         # Script to orchestrate the ETL process
- requirements.txt        # List of required libraries for the project
- Database Schema.pdf     # PDF document illustrating the database schema
- README.md               # This readme file explaining the project
```

## Database Schema

The database schema consists of one fact table and six dimension tables. Below is a detailed overview of each table:

### FactPupilAttendance Table
- Date
- AcademyID (Foreign Key to DimAcademy.AcademyID)
- GradeID (Foreign Key to DimGrade.GradeID)
- PupilID (Foreign Key to DimPupil.PupilID)
- StatusID (Foreign Key to DimStatus.StatusID)
- AttendanceID (Foreign Key to DimAttendance.AttendanceID)
- StreamID (Foreign Key to DimStream.StreamID)

### DimAcademy Table
- AcademyID (Primary Key)
- AcademyName

### DimGrade Table
- GradeID (Primary Key)
- GradeName

### DimPupil Table
- PupilID (Primary Key)
- FirstName
- MiddleName
- LastName

### DimStatus Table
- StatusID (Primary Key)
- Status

### DimAttendance Table
- AttendanceID (Primary Key)
- Attendance

### DimStream Table
- StreamID (Primary Key)
- Stream

Please refer to the [Database Schema.pdf](Database%20Schema.pdf) document for a visual representation of the database schema.

## Execution

To execute the ETL process, follow the steps below:

1. Make sure you have Python installed on your system.
2. Install the required libraries by running the following command:
   ```
   pip install -r requirements.txt
   ```
3. Execute the `main.py` script by running the following command:
   ```
   python main.py
   ```

The ETL process will be initiated, and you can monitor the progress through the logs in the `logs/logging.log` file. Once the process is completed, the transformed data will be loaded into the SQLite database `database/newglobe.db`.

## Orchestration and Scheduling

To orchestrate and schedule the ETL process, you can leverage cloud services such as Google Cloud Platform (GCP). Here's a suggested approach using GCP services:

1. Deploy the ETL scripts and data files to a cloud environment, such as a virtual machine or container.
2. Create a Google Cloud Function that triggers the execution of the ETL process. This function can invoke the `main.py` script.
3. Use Google Cloud Scheduler to schedule the execution of the Cloud Function at the desired intervals or specific times. This will automate the ETL process and ensure regular updates to the data mart.

By utilizing cloud services like Google Cloud Platform, you can achieve reliable and scalable execution of the ETL process with minimal manual intervention.
