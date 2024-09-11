### Project Summary:

**Objective**: Break apart Big Query operations into seperate scripts for timing purposes on upload. 




### Time Log:

| **Date** | **Time Spent** | **Tasks Completed** |
|----------|----------------|---------------------|
| **9/11**  | 1 hour        | Break apart Big Query operations into seperate scripts for timing purposes



### Detailed Task Descriptions:

1. **Break apart Big Query operations into seperate scripts for timing purposes**:
   - Alter directory setup for multiple batch scripts on BQ operations, change relative paths, deploy new logging
   - Change timing setup for misc_imports and test setup


5. **New Scheduling**
   - Order of operations on task scheduler now occurs as the following:

   - 3 AM. Google Cloud Bucket, DB, Tables creations of powerschool_combined, EIS folders
   - 5 AM. SFTP operations (Clever Exports, SAVVA Exports)
   - 6:50 AM. SFTP_operations_misc (Clever Imports, EasyIEP import)  
   - 6:55 AM Google Cloud Bucket, DB, Tables creations of misc_imports folder
   - 7:05 AM. SFTP_operations_local (PS_imports) 


---

### Next Steps:
- Monitor scheduled tasks to ensure they run smoothly.
- Debugging if necessary
- Regularly update version control and documentation to reflect ongoing changes.
- Relay ongoing information, and provide README_updates
---

