Cloud Data Fusion is a managed ETL pipeline service provided on the GCP Big Data stack.

Google bills customers per hour the service is active, not providing a way to turn off the service in periods of inactivity to reduce costs.

This repository contains two airflow dags that I use to create a Data Fusion instance in the morning, then perform all the batch jobs and development i need to perform and destroy the instance in the end of the day.

It can be used to lower costs. Your use case may only require that you have Data Fusion active for 2 hours, therefore reducing costs by 91%.

Example:

Basic Data Fusion Instance (per hour) = 1,8$ (April 2022)

Cost per month 24/7 = 1,8 * 24 * 30 = 1296$

Cost per month 2h per day = 1,8 * 2 * 30 = 108$