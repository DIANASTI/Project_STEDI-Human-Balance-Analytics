# Project: STEDI Human Balance Analytics

## Project Description

In this project I had to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.
To develop the solution for this project it was used  AWS IAM, AWS S3, AWS Glue, AWS Athena, Python, and Spark for sensor data that trains machine learning algorithms.

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. 
These JSON files are located under **[DataFiles](https://github.com/DIANASTI/Project_STEDI-Human-Balance-Analytics/tree/main/DataFiles)** directory in the Github repo:
- *customer/landing*
- *step_trainer/landing*
- *accelerometer/landing*


## Project Implementation
A. **Landing Zone**
In this initial stage we will use **AWS Glue Studio** to create the below 3 tables manually that contain the landing data.
Tables data was loaded from S3 bucket that contains the data as JSON format. The S3 bucket was created initially and files uploaded from **[udacity_github](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter)**
 **AWS Athena** was used to query and display the data.
  1. **customer_landing**  contains the following fields:
      - serialnumber
      - sharewithpublicasofdate
      - birthday
      - registrationdate
      - sharewithresearchasofdate
      - customername
      - email
      - lastupdatedate
      - phone
      - sharewithfriendsasofdate

![ Data display ](/Screenshots/customer_landing.png)



  2. **step_trainer_landing** contains the following fields:
      - sensorReadingTime
      - serialNumber
      - distanceFromObject

![ Data display ](/Screenshots/step_trainer_landing.png)
     
  4. **accelerometer_landing** contains the following fields:
      - timeStamp
      - user
      - x
      - y
      - z

![ Data display ](/Screenshots/accelerometer_landing.png)



B. **Trusted Zone**
Using **AWS Glue Studio** I created 3 jobs that will transform raw data from landing zone based on project requirements.
  1. **Customer_Landing_to_Trusted_project** - sanitizes the Customer data from the Website (Landing Zone) and only stores the Customer Records who agreed to share their data for research purposes (Trusted Zone) and is creating a Glue Table called customer_trusted.
![ Graph display ](/Screenshots/job_Customer_Landing_to_Trusted_project.png)
   
  2. **Accelerometer_Landing_to_Trusted_project** - sanitizes the Accelerometer data from the Mobile App (Landing Zone) - and only stores Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) and is creating a Glue Table called accelerometer_trusted.
![ Graph display ](/Screenshots/job_Accelerometer_Landing_to_Trusted_project.png)
      
  3. **StepTrainer_Landing_to_Trusted_project** - reads the Step Trainer IoT data stream (S3) and populates a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
![ Graph display ](/Screenshots/job_StepTrainer_Landing_to_Trusted_project.png)


C. **Curated Zone**
Using **AWS Glue Studio** I created other 2 jobs that join multiple trusted data sources, and apply other transformations to create curated data for furthur analysis.
  1. **StepTrainer_Landing_to_Trusted_project** - sanitizes the Customer data (Trusted Zone) and creates a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
![ Graph display ](/Screenshots/job_StepTrainer_Landing_to_Trusted_project.png)

  2. **MachineLearning_Curated_project** - creates an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and makes a glue table called machine_learning_curated.
![ Graph display ](/Screenshots/job_MachineLearning_Curated_project.png)




**NOTES**
All tables definition can be found under **[DDL](https://github.com/DIANASTI/Project_STEDI-Human-Balance-Analytics/tree/main/DDL)**
All Glue jobs scripts can be found under **[Python_scripts](https://github.com/DIANASTI/Project_STEDI-Human-Balance-Analytics/tree/main/GlueJobs)**
For AWS Glue to act on your behalf to access S3 and other resources, you need to grant access to the Glue Service by creating an IAM Service Role that can be assumed by Glue:
```
aws iam create-role --role-name my-glue-service-roleds --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'
```




