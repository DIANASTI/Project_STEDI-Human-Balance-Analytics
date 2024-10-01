# Project: STEDI Human Balance Analytics

## Project Description

In this project I had to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.
To develop the solution for this project it was used  AWS IAM, AWS S3, AWS Glue, AWS Athena, Python, and Spark for sensor data that trains machine learning algorithms.

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. 
These JSON files are located under [DataFiles](https://github.com/DIANASTI/Project_STEDI-Human-Balance-Analytics/tree/main/DataFiles) directory in the Github repo:
- *customer*
- *step_trainer*
- *accelerometer*


## Implementation
1. Landing Zone
2. Trusted Zone
3. Curated Zone








This folder should contain public project starter code.

Upload the directories to your S3 bucket and use them to create your Glue tables.

When you have completed this project, your S3 bucket should have the following directory structure:

```
customer/
- landing/
- trusted/
- curated/
accelerometer/
- landing/
- trusted/
step_trainer/
- landing/
- trusted/
- curated/
```


