### Project Summary:

**Objective**: Implement data flows to and from vendors and Big Query

### Time Log:

| **Date**       | **Time Spent** | **Tasks Completed**                                     |
|----------------|----------------|---------------------------------------------------------|
| **10/29/2024** | 3 hours        | - SAVVAS scores operation                               |
| **10/30/2024** | 3 hours        | - OTUS implementation, Roster Files Sending to Ellevation|


### Detailed Task Descriptions:

1. **SAVVAS Operation**:
   - Automated latest pull of SAVVAS Scores (two files) into misc_import folder in order to be populated into GCS Buckets, and Big Query

2. **OTUS Implementation**:
    - Amongst SAVVAS Scores files, each of these files are automated to send to OTUS SFTP folder on a daily basis
    - Bring down OTUS_Attendance_Discpliine table from BQ on a weekly basis to send to OTUS SFTP folder. 

3. **Ellevation Rostering Implementation**
    - Implement BQ Rostering Views to send to Ellevation on a daily basis
    - Staff-Roster, Student-Course-Schedules, Student-Demographics

   
3. **Scheduling Setup**:

   - **3:00 AM Bigquery_operations**: Google Cloud Bucket, DB, and table creation for `powerschool_combined` and EIS folders.
   - **5:00 AM SFTP_operations**: SFTP operations for Clever, SAVVAS, Ellevation, & OTUS exports. 
   - **6:50 AM SFTP_operations_misc**: SFTP miscellaneous operations for Clever Imports, EasyIEP Import, and SAVVAS scores.
   - **6:55 AM Bigquery_operations_misc**: Google Cloud Bucket, DB, and table creation for `misc_imports` folder.
   - **7:05 AM SFTP_operations_local**: SFTP local operations (`PS_imports`).
   - **7:30 AM EIS_adm_audit_student_membership**: Selenium operations.
   - **7:35 AM EIS_school_error_reports**: Selenium operations.

---

### Next Steps:
- Monitor scheduled tasks to ensure smooth operation.
- Debug as necessary.
- Regularly update version controlled code and documentation (README) to reflect ongoing changes.

