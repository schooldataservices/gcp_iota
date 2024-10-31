### Project Summary:
**Objective**: Implement proper SFTP pushes & pulls for Clever and SAVVAS, stack files for Bluff & ASD to singular files, ensure EIS_Selenium processes are set to download properly, ensure BigQuery population of all necessary local files via Google API. Debug & log out local scripts and version control all files in Bitbucket with detailed comments.

### Task Breakdown:

#### Refactoring SFTP Module
- **Objective**: Modify the SFTP module to work for Clever and SAVVAS.
- **Tasks**:
  - Pull Google addresses from Clever directly into the `powerschool_combined` folder so it can be created in BigQuery tables.
  - Pull down BigQuery views, and populate as local files and push to Clever and SAVVAS SFTP folders as txt or csv files in specific naming convention below. 

```python
#All BQ tables that are begin queryed are the keys. 
#How they are saved in the local dir are the values
clever_dictionary = {'Clever_schools':'schools.csv',
                    'Clever_students':'students.csv',
                    'Clever_teachers':'teachers.csv',
                    'Clever_sections':'sections.csv',
                    'Clever_enrollments':'enrollments.csv',
                    'Clever_staff': 'staff.csv'
                    }

savva_dictionary = {
                    'SAVVAS_CODE_DISTRICT': 'CODE_DISTRICT.txt',
                    'SAVVAS_SCHOOL': 'SCHOOL.txt',
                    'SAVVAS_STUDENT': 'STUDENT.txt',
                    'SAVVAS_STAFF': 'STAFF.txt',
                    'SAVVAS_PIF_SECTION': 'PIF_SECTION.txt',
                    'SAVVAS_PIF_SECTION_STAFF': 'PIF_SECTION_STAFF.txt',
                    'SAVVAS_PIF_SECTION_STUDENT': 'PIF_SECTION_STUDENT.txt',
                    'SAVVAS_ENROLLMENT': 'ENROLLMENT.txt',
                    'SAVVAS_ASSIGNMENT': 'ASSIGNMENT.txt'
                }
```

#### Task Scheduler Setup
- **Objective**: Implement new task scheduling for SFTP operations and BigQuery updates.
- **Tasks**:
  - Set up new schedules for Biq Query at 3 AM and SFTP ops at 5 AM.

### Time Log:

| **Date** | **Time Spent** | **Tasks Completed** |
|----------|----------------|---------------------|
| **7/1**  | 2 hours        | Initial setup and refactoring for SFTP module. |
| **7/2**  | 5 hours        | Continued SFTP module refactoring and testing. |
| **7/4**  | 1 hour         | Implemented Clever SFTP send start. |
| **7/10** | 1 hour         | Set up infrastructure to write to Clever and SAVVAS SFTP. |
| **7/11** | 1 hour         | Worked on SAVVAS pull-down from Google Cloud, replicated files for send, and implemented SFTP connection pooling. |
| **7/12** | 2 hours        | Debugging and implementing Clever send. |
| **7/16** | 3 hours        | Integrated students table into EIS Cohort Tracking Scrape, removed old logic, enhanced logging, applied EIS application changes, and reorganized EIS Selenium Python files. Task scheduler setups|
| **7/30** | 1 hour        | Adjusting SFTP sends for Clever, debugging, testing, updating version |

### Detailed Task Descriptions:

1. **Refactoring SFTP Module (Clever & SAVVAS)**:
   - Adjusted the module to handle connections and data transfers (import & exports) for both Clever and SAVVAS platforms.
   - Ensured data is correctly pushed, & pulled from SFTP & populated in BigQuery.
   - Integrated additional logging and error handling for robustness.

2. **BigQuery Population**:
   - Restructured how data is populated in BigQuery through scalable scheme to accommodate new sources and formats.
   - Created script to bring together ASD and Bluff schools as singular file and uploaded them to BigQuery daily.

3. **EIS Selenium**:
   - Pulled down necessary student data from BQ, and implemented into EIS scrape to get prior enrollment history. Set up process to run on a schedule for the first of the month, and populated in BQ EIS db. 


4. **Task Scheduler Setup**:
   - Configured new tasks in the scheduler to run BigQuery updates at 3 AM and SFTP operations at 5 AM.
   - EIS processes run between 2 & 3 AM. 
   - Any changes?

5. **Miscellaneous**:
   - Updated version control systems in Bitbucket with the latest changes.
   - Enhanced logging mechanisms for better traceability and debugging.
   - Applied organizational changes to EIS Selenium Python files to streamline future development.

---

### Next Steps:
- Monitor scheduled tasks to ensure they run smoothly.
- Debugging if necessary
- Regularly update version control and documentation to reflect ongoing changes.

---

