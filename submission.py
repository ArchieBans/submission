import requests
import json
import pyodbc


def getDataFromAPI(accesstoken):
    accesstoken = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Imwzc1EtNTBjQ0g0eEJWWkxIVEd3blNSNzY4MCIsImtpZCI6Imwzc1EtNTBjQ0g0eEJWWkxIVEd3blNSNzY4MCJ9.eyJhdWQiOiJodHRwczovL29yZ2Q4ZWYzYTRiLmNybS5keW5hbWljcy5jb20vIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvMzc1NGZjNzYtYzliYi00MDRhLTlhMjktNmE1ZDFjNTMyYzgxLyIsImlhdCI6MTYzNTI2NDYyOCwibmJmIjoxNjM1MjY0NjI4LCJleHAiOjE2MzUyNjg1MjgsImFjciI6IjEiLCJhaW8iOiJFMlpnWU9oM2NIbGk2Umw4WGpIMWc1bmtqd2svbTI0M2xLM2pDT3hjcWp4eldkS1NFR3NBIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjUxZjgxNDg5LTEyZWUtNGE5ZS1hYWFlLWEyNTkxZjQ1OTg3ZCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiVGVzdCBBY2NvdW50IiwiZ2l2ZW5fbmFtZSI6IktPUkUgSW50ZXJ2aWV3IiwiaXBhZGRyIjoiMjQuODAuNDEuMTQ2IiwibmFtZSI6IktPUkUgSW50ZXJ2aWV3IFRlc3QgQWNjb3VudCIsIm9pZCI6ImQxNDdhOTA5LWM3ZTctNGQwZC1iNjRjLWE4MWI5OTIxZDE2MSIsInB1aWQiOiIxMDAzMjAwMTk5ODdCMkJBIiwicmgiOiIwLkFUZ0FkdnhVTjd2SlNrQ2FLV3BkSEZNc2dZa1UtRkh1RXA1S3FxNmlXUjlGbUgwNEFEVS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiI1R2IyRzlpLU8zbUhUQWhRU2xVUTBsQWI1aWtqOEhuaTFuUVUyNkJWZkc4IiwidGlkIjoiMzc1NGZjNzYtYzliYi00MDRhLTlhMjktNmE1ZDFjNTMyYzgxIiwidW5pcXVlX25hbWUiOiJrb3JldGVjaGludGVydmlld0Brb3JlaW50ZXJhY3RpdmUub25taWNyb3NvZnQuY29tIiwidXBuIjoia29yZXRlY2hpbnRlcnZpZXdAa29yZWludGVyYWN0aXZlLm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6IlNPMXpFZndSTzAyOElMTHJneXNsQUEiLCJ2ZXIiOiIxLjAifQ.qxxozkMceRi1ktcoIwxYADVaUnO_7IbYCam6IFa8zY2Ah_NWDCnpLDrI7w1eMOdqcQgG69g8Hhc0LwUKxc531h8UiXFqjyZ5dVpgMIvdhzUTvwtwULE-YCFy1egvgmvaATEBXuEhJPYtikS2oeJ6Hat-RGcs1PGaWkH3DJq4OB4tnRABv9nqRWDMjHlCLijWCHcIQKNWcUrd38bsRps2B2jCX6vEMlUEhsilRPbIAaROujO2fhtSmWHTsSTQ27-aKdMBHxj6gF9niWCrt3BqIVw0Qis9UDNBP0DZoDKOWcwq632wZgpEr0DvEMqF0k-RpuVQjeviHUoxzWx40_lchg'
    if(accesstoken!=''):
        #prepare the crm request headers
        crmrequestheaders = {
            'Authorization': 'Bearer ' + accesstoken,
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
            'Prefer': 'odata.maxpagesize=500',
            'Prefer': 'odata.include-annotations=OData.Community.Display.V1.FormattedValue'
        }
        
        #set these values to query your crm data
        crmwebapi = 'https://orgd8ef3a4b.crm.dynamics.com/api/data/v9.0/' #full path to web api endpoint
        crmwebapiquery = '/contacts' #web api query (include leading /)
        
        #make the crm request
        crmres = requests.get(crmwebapi+crmwebapiquery, headers=crmrequestheaders)
 
        try:
            #get the response json
            crmresults = crmres.json()
            
        except KeyError:
            #handle any missing key errors
            print('Could not parse CRM results')   
            
    return crmresults        
def insertDataIntoDynamicsTable():
    data = getDataFromAPI('')
    
    server = 'koretechinterview.database.windows.net' 
    database = 'KORESampleDatabase' 
    username = 'koreinterview' 
    password = 'a8Xp6Pz6&$TR' 
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()    
    
    # crsr.fast_executemany = True For bulk load
    
    sql_query_insert = "Insert into dynamics.dynamics_staging (contact_id,sourceId,title,firstName,lastName,email,companyName,createdOn,modifiedOn,wasContacted,isDynamics,isSalesLT,address1,city,state,zip,country,priPhoneNumber,donotphone) VALUES (?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    sql_query_customer_table = "Insert into dynamics.dynamics_staging (contact_id,sourceId,title,firstName,lastName,email,companyName,createdOn,modifiedOn,wasContacted,isDynamics,isSalesLT,address1,city,state,zip,country,priPhoneNumber,donotphone) select c.rowguid ,'None',cav.title,cav.firstName,cav.lastName,cav.email,cav.companyName,GETDATE() ,GETDATE() ,'0',0,1,cav.address1,cav.city,cav.state,cav.zip,cav.country,cav.priPhoneNumber,'0'  from SalesLT.customerAddressView cav , SalesLT.Customer c where cav.email = c.EmailAddress; "
    sql_query_insert_leads_staging = "Insert into dynamics.Leads_staging (id,sourceId,title,firstName,lastName,email,companyName,createdOn,modifiedOn,wasContacted,isDynamics,isSalesLT,address1,city,state,zip,country,priPhoneNumber) select contact_id,sourceId,title,firstName,lastName,email,companyName,createdOn,modifiedOn,wasContacted,isDynamics,isSalesLT,address1,city,state,zip,country,priPhoneNumber from (select *,ROW_NUMBER() OVER(PARTITION by email order by isDynamics) rn from dynamics.dynamics_staging ds) a where rn = 1 and donotphone = 0"
    sql_query_insert_dbo_leads = "Insert into dbo.Leads (id,sourceId,title,firstName,lastName,email,companyName,createdOn,modifiedOn,wasContacted,isDynamics,isSalesLT,address1,city,state,zip,country,priPhoneNumber) select * from dynamics.Leads_staging"
    
    truncate_query_dynamics_staging = "Truncate table dynamics.dynamics_staging;"
    truncate_query_leads_staging = "Truncate table dynamics.Leads_staging;"
    truncate_query_leads = "Truncate table dbo.Leads"
    
    cursor.execute(truncate_query_dynamics_staging)
    cursor.execute(truncate_query_leads_staging)
    cursor.execute(truncate_query_leads)
    cnxn.commit()
    
    
    for x in data['value']:
        record = (x['contactid'], x['_parentcustomerid_value'], x['jobtitle'], x['firstname'],x['lastname'], x['emailaddress1'], x['company'],x['createdon'],x['modifiedon'],'0',1,0,x['address1_line1'],x['address1_city'],x['address1_stateorprovince'],x['address1_postalcode'],x['address1_country'], x['telephone1'],x['donotphone'])
        cursor.execute(sql_query_insert, record)
        cnxn.commit()
    
    
    cursor.execute(sql_query_customer_table)
    cursor.execute(sql_query_insert_leads_staging)
    cursor.execute(sql_query_insert_dbo_leads)
    cnxn.commit()
        
def createStagingTableForDynamics():
    sql_query_create_dynamics = "Create table dynamics.dynamics_staging (contact_id uniqueidentifier NULL, 	sourceId nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	title nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	firstName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	lastName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	email nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	companyName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	createdOn datetime NULL, 	modifiedOn datetime NULL, 	wasContacted bit NULL, 	isDynamics bit NULL,isSalesLT bit NULL, 	address1 nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	city nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	state nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	zip nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	country nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	priPhoneNumber nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, donotphone nvarchar(20));"
    sql_query_drop = "Drop table dynamics.dynamics_staging"
    sql_query_drop_leads = "Drop table dynamics.Leads_staging"
    sql_query_create_leads_staging = "CREATE table dynamics.Leads_staging (id uniqueidentifier NULL, 	sourceId nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	title nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	firstName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	lastName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	email nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	companyName nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	createdOn datetime NULL, 	modifiedOn datetime NULL, 	wasContacted bit NULL, 	isDynamics bit NULL, 	isSalesLT bit NULL, 	address1 nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	city nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	state nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	zip nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 	country nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, priPhoneNumber nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL );"    
    server = 'koretechinterview.database.windows.net' 
    database = 'KORESampleDatabase' 
    username = 'koreinterview' 
    password = 'a8Xp6Pz6&$TR' 
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()    
    
    cursor.execute(sql_query_drop)
    cursor.execute(sql_query_drop_leads)
    
    cursor.execute(sql_query_create_leads_staging)
    cursor.execute(sql_query_create_dynamics)
    cnxn.commit()


#If couldn't make a DAG to do this but if the enviornment was working there is a seperate file named DAG that
# has compiled(no errors) source code with DAG initializations.

 
createStagingTableForDynamics();
insertDataIntoDynamicsTable();        

