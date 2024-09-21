### Project Summary:

**Objective**: Break apart BigQuery operations into separate scripts for timing purposes on upload. Set up EIS Selenium scripts to automate Google Chrome browser in acquiring files for TN schools daily to be stacked and populated in Google Cloud (ADM Audit, Student Membership, School Error Reports).

### Time Log:

| **Date**       | **Time Spent** | **Tasks Completed**                                     |
|----------------|----------------|--------------------------------------------------------|
| **09/11/2024** | 1 hour         | - Broke apart BigQuery operations into separate scripts |
| **09/19 - 09/20** | 5 hours    | - Worked on EIS Selenium scripts                        |


### Detailed Task Descriptions:

1. **Breaking apart BigQuery operations into separate scripts for timing**:

   - Altered directory setup for multiple batch scripts on BQ operations.
   - Changed relative paths and deployed new logging.
   - Updated timing setup for `misc_imports` and tested the new configuration.

2. **EIS Selenium Scripts**:

   - Configured the server’s Chrome browser to prevent updates and integrated a newly configured WebDriver setup.
   - Accounted for slowdowns in the EIS website when navigating and launching applications.
   - Handled revolving XPath elements for schools during website refreshes.
   - Configured logging, file paths, scheduling, and batch scripts.
   - https://bitbucket.org/iota-schools/eis_selenium/src/main/ 

3. **New Scheduling Setup**:

   - **3:00 AM**: Google Cloud Bucket, DB, and table creation for `powerschool_combined` and EIS folders.
   - **5:00 AM**: SFTP operations for Clever and SAVVAS exports.
   - **6:50 AM**: SFTP miscellaneous operations (Clever Imports, EasyIEP Import).
   - **6:55 AM**: Google Cloud Bucket, DB, and table creation for `misc_imports` folder.
   - **7:05 AM**: SFTP local operations (`PS_imports`).
   - **7:30 AM**: EIS ADM Audit & Student Membership Selenium operations.
   - **7:35 AM**: EIS School Error Reports Selenium operations.

---

### Next Steps:
- Monitor scheduled tasks to ensure smooth operation.
- Debug as necessary.
- Regularly update version control and documentation (README) to reflect ongoing changes.

