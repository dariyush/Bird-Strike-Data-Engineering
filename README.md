# Data Engineering for Bird Strike Data
The Analytics team have a requirement to generate some insights and business reporting on bird strikes occurring across the U.S.  Currently, a daily csv file from is received from a supplier which contains the new and updated records for the previous day.  A consolidated weekly file is also received which contains the full data set.  We would like to make this data available to the Analytics team to allow them to deliver on their known business reporting requirements as well as allow them to run analytic models to discover further (currently unknown) insights.

## Some considerations:
-	The Analytics Team currently predominantly use SQL.  They would like to continue to access and query the data using SQL.
-	The Analytics Team would like to persist all history as they are not yet sure how they may use the data.
You have been tasked with designing and developing a process to ingest these files on a repeatable basis, store the data, and make the data available for querying by the Analytics team.

## Design and develop a process to ingest and store the data, as well as apply some business rules to enrich the data, and then perform some analysis over the data.  
This should include:
1.	Propose a design to load and store the data as well as make the data available for reporting and analytics.
2.	Build a process to ingest and store the data, in raw form. This should also consider the changing structure of the files. 
3.	Implement the following validation rules to help ensure data quality.  Data Analytics want some form of indication for records that don’t meet the data quality requirements:
  a.	Values in the Feet above ground file do not fall within the corresponding altitude bin field.  
  b.	Review the data and propose 1 of your own Data Quality checks.  Why do you think it’s an important check?
4.	Implement the following business rules to enrich the data.  
  a.	How can you better model the data in the “Conditions: Precipitation” field to make it easier to use for analysis and reporting?

# Data Augmentation
As part of the assignment, please also answer the following questions (you do not have to develop any code to answer these questions):
1.	Assuming you could make suggestions to the supplier to change how they provide the data:
  a.	What changes would you propose and why?
  b.	How would this influence your design?
2.	What additional third-party data sets would you attempt to source to augment the bird strikes data to provide better insights into this data?
  a.	How could you design your solution so that the Analytics Team could be enabled to load these additional data sets without relying on the Data Engineering team to undertake any development?

# Data Modelling
1.	Design a data model optimised for presenting this data through a BI tool.  Why would you choose this particular type of data model?
2.	Provide a design for loading data into this data model.  Note that you do not actually have to populate this data model.
3.	Propose at least 2 BI tools that could be used and what questions you would ask to evaluate which tool to use (you do not actually have to answer the questions).

# Data Analysis
1.	Use the data to answer the following questions (you can answer the questions directly from the latest csv file.  You don’t have to populate / use your data model):
  a.	Which departure airport has the highest number of bird strikes?
  b.	What is the overall trend in reported bird strikes over time?  
  c.	Why do you think the trend exists?
2.	Propose 2 of your own questions – and answers.  Why would these be important for the business.

