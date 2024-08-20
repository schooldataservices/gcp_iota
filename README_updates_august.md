### Project Summary:
**Objective**: Alter python processes to move all automated buckets, and table creation to new region as us-central1. 
Incorporate EasyIEP SFTP transfer to Google Cloud Population.
Seperate Clever, and EasyIEP imports out into seperate file.




### Time Log:

| **Date** | **Time Spent** | **Tasks Completed** |
|----------|----------------|---------------------|
| **8/7**  | 3 hours        | Alter python processes to move all automated buckets, and table creation to new region as us-central1. Add in EasyIEP SFTP transfer and integrate logic into codebase |
| **8/19** | 3 hours      | Branch out Clever, and EasyIEP imports out into seperate file from main. Pull down BQ views into Local SFTP for PS password retrieval. Implement new scheduling in task scheduler.



### Detailed Task Descriptions:

1. **Move Buckets and Tables to us-central1**:
   - Download Google Cloud SDK (Software Development Kit) to local to apply new permissions to service account to properly create buckets and tables in new regions
   - Alter python class instance to accept location as argument passed down from functions
   - Delete prior tables and buckets to allow for new creations
   - Change BQ view pulldowns to read from roster_files db
   - Enhanced logging for transparency on location creation 

2. **Add in EasyIEP SFTP transfer setup & integrate into current processes**
   - Alter the buckets.py module to be able to download specific file from SFTP rather than all files
   - Have functions work to download all files for SAVVA and specific file for easyIEP while following naming conventions

3. **Seperate Clever, and EasyIEP imports out into seperate file** 
   -Branch out from main script and make seperate python file (misc_sftp_operations) and batch script (misc_sftp_ops.bat) for these two processes to run at 6:50 AM every day
   -Implement Python hierarchy and organization for proper imports, logs, testing. Delete remnants of these files within Powerschool-combined SFTP folder, bucket and table.

4. **Pull Down BQ Views into Local SFTP for PS Password Retrival**  
   - Branch out from main script and make seperate python file (local_sftp_operations.py) and batch script (local_sftp_ops.bat) for these processes to run at 7:05 AM every day. 
   - Implement Python hierarchy and organization for proper imports, logs, testing.
   - Files now sent to local sftp folder PS_imports

5. **New Scheduling**
   - Order of operations on task scheduler now occurs as the following:

   - 3 AM. Google Cloud Bucket, DB, Tables creations of powerschool_combined, EIS, misc_imports 
   - 5 AM. SFTP operations (Clever Exports, SAVVA Exports)
   - 6:50 AM. SFTP_operations_misc (Clever Imports, EasyIEP import)  
   - 7:05 AM. SFTP_operations_local (PS_imports) 


---

### Next Steps:
- Monitor scheduled tasks to ensure they run smoothly.
- Debugging if necessary
- Regularly update version control and documentation to reflect ongoing changes.
- Relay ongoing information, and provide README_updates
---

