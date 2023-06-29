# nypd_calls_for_service

This is a project to download and analyze NYC Open Data's Historic 911 Calls for Service Data. 

This project was written in python 3.11.1. You will need PostgreSQL to run this, I am running version 15. All of the SQL statements are executed within the python scripts.

I have included a requirements.txt you can download all the dependencies when you create your venv. 

Start with the postgres_config.py, there you will need to enter your postgres credentials. 

Then move to create_db_and_schema.py where you will create the database and schema, this should only be run once, but it has error handling that won't create duplicates if you need to run again.

Then you can move to the nypd_api_call.py and get the data. This generally runs overnight when I start it so keep that in mind. I have added print statements to track your progress.

Then, once you have everything, you can move to the analysis_notebook.ipynb and run the data through some analysis.

The async api call script is something I"m working to speed up the download, since there are no limits on the api. The weather data script is also something I'm working on and will add to the analysis notebook soon, for some visualizations.

Let me know if you have any issues or ideas!