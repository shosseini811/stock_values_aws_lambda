This code demonstrates how to create an automated system 
that monitors Apple's stock prices in real-time using AWS Lambda, 
CloudWatch, and RDS. 
The system uses a Python script to fetch the latest stock prices from Yahoo Finance 
and stores them in a PostgreSQL database hosted on AWS RDS. 
AWS Lambda runs the script, and CloudWatch is utilized to schedule the script to execute every minute. 
This serverless architecture is cost-effective and scales automatically, 
making it an excellent resource for traders, investors, and data analysts. 
The principles can also be adapted for monitoring other stocks or conducting more complex data analysis. 