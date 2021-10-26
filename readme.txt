Readme document for data engineering submission.
Dependencies:

pyodbc, json, requests(submission.py)
pyodbc, json, requests,airflow and datetime

This folder contains 4 files:
1. submission.py This file is the demo file, given the installed requirements and a new access token, this 
                 will do the tasks mentioned in outline except for hourly scheduling.
                 
2. dag_submision.py This file is DAG version of submission.py. Given an airflow server with requirements installed
                    this file will do the work on hourly basis.

3. Readme file.
4. reponse.html





**NOTE - 
crmorg = 'https://orgd8ef3a4b.crm.dynamics.com' #base url for crm org
clientid = '51f81489-12ee-4a9e-aaae-a2591f45987d' #application client id
username = 'koretechinterview@koreinteractive.onmicrosoft.com' #username
userpassword = 'Cah67048' #password
tokenendpoint = 'https://login.microsoftonline.com/common/oauth2/authorize?resource=https://orgd8ef3a4b.crm.dynamics.com/' #oauth token endpoint
 
#set these values to query your crm data
crmwebapi = 'https://orgd8ef3a4b.crm.dynamics.com/api/data/v9.0/' #full path to web api endpoint
crmwebapiquery = '/contacts' #web api query (include leading /)
 
#build the authorization token request
tokenpost = {
    'client_id':clientid,
    'resource':crmorg,
    'username':username,
    'password':userpassword,
    'grant_type':'password'
}
 
#make the token request
tokenres = requests.get(tokenendpoint, data=tokenpost)
  
Ideally we should have a new access token everytime we request the data but requests module has made some changes in 
the header file that comes back and we are not able to verify the bearer. So to test the code I made a fresh token
from postman.

The file reposne.html is the return value of requests.get(tokenendpoint, data=tokenpost). Ideally we should be getting 
a file where we can parse the 'access_token' key, but the format is changed so I didn't had enough time to hack this file.


Data Mapping.

Initially the JSON request that we get from donations are mapped out into the table called dynamics_staging.
Here is the create statement for the staging table.

Create table dynamics.dynamics_staging (contact_id uniqueidentifier NULL, 	sourceId nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	title nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	firstName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	lastName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	email nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	companyName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	createdOn datetime NULL, 	modifiedOn datetime NULL, 	wasContacted bit NULL, 	isDynamics bit NULL,isSalesLT bit NULL, 	address1 nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	city nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	state nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	zip nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	country nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	priPhoneNumber nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, donotphone nvarchar(20));

The mapping from the JSON document is as follows.

['contactid']['_parentcustomerid_value']['jobtitle']['firstname']['lastname']['emailaddress1']['company']['createdon']['modifiedon'],'0',1,0,['address1_line1']['address1_city']['address1_stateorprovince']['address1_postalcode']['address1_country']['telephone1']['donotphone']

After that a combine select statement is run on the dynamics_staging table and customer table which combines the data and populates the 
Leads_staging table. Once this table is populated then we take the unique email address entries and upload them to dbo.Leads_staging

