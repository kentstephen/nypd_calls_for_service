# nypd_calls_for_service

  
This is a project to download and analyze NYC Open Data's Historic 911 Calls for Service Data. The end goal will be to analyze the volume of call data against winter/summer months and finally to download the corresponding temperature data to see if there is any correlation there.

## Technologies Used:

 - Python 3.11.1
 - PostgreSQL 15. PGadmin or another IDE could be useful but all SQL statements are in the Python scripts.

 

  ## Steps to run the application:

I have included a requirements.txt you can download all the dependencies when you create your virtual environment.

  
 1. Start with the [A_postgres_config.py](A_postgres_config.py), there you will need to enter your postgres credentials.

 2. Then move to [B_create_db_and_schema.py](B_create_db_and_schema.py) where you will create the database and schema, this should only be run once, but it has error handling that won't create duplicates if you need to run again.

 3. Then you can move to the [C_nypd_api_call.py](C_nypd_api_call.py) and get the data. This generally runs overnight when I start it so keep that in mind. I have added print statements to track your progress. If your ETL gets interrupted, check the row count in Postgres and then set the skip function to that number and run the program again.

 4. Then, once you have everything, you can move to the [D_analysis_notebook.ipynb](D_analysis_notebook.ipynb) and run the data through some analysis.

## In development, coming soon:

  The async api call script is something I"m working to speed up the download, since there are no limits on the api. The weather data script is also something I'm working on and will add to the analysis notebook soon, for some visualizations.
  

Let me know if you have any issues or ideas!
