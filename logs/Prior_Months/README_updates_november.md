### Project Summary:

**Objective**: Process Big Query's PS_Imports tables to files and export them on a daily basis to Clevers SFTP

### Time Log:

| **Date**       | **Time Spent** | **Tasks Completed**                                     |
|----------------|----------------|---------------------------------------------------------|
| **10/20/2024** | 2 hours        | - Big Query tables from PS_Imports to Clever SFTP       |

### Detailed Task Descriptions:

1. ** Operation**:
    - Processing Big Query's PS_Imports tables to files and exporting them on a daily basis to Clevers SFTP
    - Occurs at 7:05 AM Central

2. **Scheduling Setup**:

   - **3:00 AM Bigquery_operations**: Google Cloud Bucket, DB, and table creation for `powerschool_combined` and EIS folders.
   - **5:00 AM SFTP_operations**: SFTP operations for Clever, SAVVAS, Ellevation, & OTUS exports. 
   - **6:50 AM SFTP_operations_misc**: SFTP miscellaneous operations for Clever Imports, EasyIEP Import, and SAVVAS scores.
   - **6:55 AM Bigquery_operations_misc**: Google Cloud Bucket, DB, and table creation for `misc_imports` folder.
   - **7:05 AM SFTP_operations_local**: SFTP local operations (`PS_imports`).

---

### Next Steps:
- Monitor scheduled tasks to ensure smooth operation.
- Debug as necessary.
- Regularly update version controlled code and documentation (README) to reflect ongoing changes.