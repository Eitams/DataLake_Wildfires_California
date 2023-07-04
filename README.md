# DataLake_Wildfires_California
*Academic project on Data Lake and Data Warehouse*
## Introduction
This project has been divided into two distinct phases. The initial phase focuses on establishing a data lake centered around a specific topic and involves the collection of data from more than three different sources. The subsequent phase revolves around leveraging this data lake effectively. The data lake itself is hosted on Amazon Web Services (AWS), so within this repository, you will discover lambda functions and Apache Airflow DAG scripts that can only be executed within their respective AWS environments. Additionally, there are separate folders containing Jupyter Notebooks or data that were utilized to assist in the development of the data lake. Lastly, the Python scripts present in the repository serve as functional components responsible for triggering data retrieval from APIs and loading it into an AWS-hosted RDS table.

## Goal
Our chosen focus of study is Wildfires in California. The ultimate objective is to create a real-time visualization tool that serves as a valuable resource for decision-makers and first responders. This tool will incorporate comprehensive data, including information on ongoing fires, thermal anomalies, as well as predictions and projections. The data lake is constructed with two pipelines: the historical pipeline, which includes data on past wildfires, weather patterns, and drought, and the real-time pipeline, which captures data on current thermal anomalies and weather conditions. Our goal is to develop a predictive model using machine learning concepts to forecast the progression of wildfires, such as estimating the magnitude of a fire based on the prevailing weather conditions.

## Real time pipeline
![image](https://github.com/Eitams/DataLake_Wildfires_California/assets/62335786/40086811-62e4-4111-b13d-c5012c4d8961)

## Historical data pipeline
![image](https://github.com/Eitams/DataLake_Wildfires_California/assets/62335786/7417eea7-3a68-4a64-96f0-89cadfbada2c)


## GitHub content

**aws_lambda:** Folder contain lambda function to process and load data. Those files are supposed to run only in AWS. There is environment variables and other functions, only declared in the AWS environment. Therfore, it is impossible to run those files, it is only for information purposes. 

**dag:** Folder contain DAG script to execute lambda function in AWS. Those files are also only for information, they cannot be run in a python enviroment, only Apache AirFLow.

**data:** Folder conatin data, not supposed to be displayed (.gitignore)

**notebooks:** Folder contain Jupyter Notebooks. Help and visualization tools to build datalake, not formatted and not clean.

**historical_weather_data.py:** python script to run to access wildfires data table hosted in AWS, access locations and date to get weather data, from Visaul Crossing API, and then load those data in a RDS table also hosted by AWS but a different account. Endpoint, database name, user and APIkey also declared in as environment variables in a .env file (not display cause of .gitignore). 

**utils.py:** function used to build API query and extract information from JSON files, used in <historical_weather_data.py'>

**Drought_history.ipynb:** Jupyter Notebook for loading historical drought indices data from API to RDS table.

**Wildfire_History.ipynb:** Jupyter Notebook for loading historical wildfires data from ArcGIS API to RDS table.

**credentialsForrer.json:** JSON file with AWS access point

**.env:** not displayed for security reason, contact me for access

## Final Output

Here, you can find different visualization from Tableau

*Real-Time pipeline*
https://public.tableau.com/views/CaliforniaWildfires_16542072566010/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link

*Historical pipeline*
Wildfires in California: https://public.tableau.com/views/Californian_wildfires/Wildfires?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link 

Monthly Analysis: https://public.tableau.com/views/Californian_wildfires/Monthlyanalysis?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link 

Weather and Drought overview: https://public.tableau.com/views/Californian_wildfires/WDOverview?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link 
