# Reporting Server
It runs scheduled tasks to generate reports.

## Configurations
1. email host and login info.
2. IEX API key.
3. Etherscan API key.
4. Postgresql database configuration.

## Deployment To Linux Server
1. clone/pull the repo to the Linux dir you want to run the reports in.  Copy the \_secrets.py file to creports folder.
2. run ``pip install -r requirements.txt``.  If issues with psycopg2, you need to install psycopg2-binary instead.



 

