# Processing-Data-Customer-ETL-Cloud
- Data from an online media company.
- Tech stack: PYSPARK, AZURE DATABASE.
# INPUT: 
- Data customer from LogContent: 30 file JSON / 1 file ~ 300 Mb , all ~ 8 GB.
- Data customer from LogSearch: 28 folder data / file data PARQUET  ~ 150 MB.

# Output:
- ETL and process data from raw data to data can be analyzed.
- Save Processed data to 4 tables on Azure Database.
- Analysis data --> write a report.

# List file:
- _Processing_Data_LogContent.py_      ---->  Script python - ETL & Processing & Import Data LogContent to Azure Database.
- _Processing_Data_LogSearch.ipynb_    ---->  Script python - ETL & Processing & Import Data LogSearch to Azure Database.
- _process_log_search.py_              ---->  Explain script in jupyter Notebook - ETL & Processing & Import Data LogSearch to Azure Database.
- _Phân Tích.docx_                     ---->  Report.
