### Project Summary:
**Objective**: Alter python processes to move all automated buckets, and table creation to new region as us-central1. 
Incorporate EasyIEP SFTP transfer to Google Cloud Population.




### Time Log:

| **Date** | **Time Spent** | **Tasks Completed** |
|----------|----------------|---------------------|
| **8/7**  | 3 hours    | Alter python processes to move all automated buckets, and table creation to new region as us-central1. Add in EasyIEP SFTP transfer and integrate logic into codebase |


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
---

### Next Steps:
- Monitor scheduled tasks to ensure they run smoothly.
- Debugging if necessary
- Regularly update version control and documentation to reflect ongoing changes.
- Relay ongoing information, and provide README_updates
---

