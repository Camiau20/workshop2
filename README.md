# ETL Pipeline Workshop with Apache Airflow

This project is a workshop where we go through the creation of an ETL (Extract, Transform, Load) using Apache Airflow. In this repository it's the process of how to extract data from a csv file (Spotify songs dataset) and a database (Grammy Awards nominees), perform data transformations, merge the transformed data, and load it  into a database and store it in Google Drive as a CSV file. Finally, with this clean data we created some visualizations that have the objective of analyzing the relation between the feelings a song can produce in people and the probability of being nominated in the Grammy Awards, but you can do any analyisis you want to present the information with value.

## Prerequisites

Before deploying this project, you need the following prerequisites:
- An Ubuntu 20.04 environment.
- A Python virtual environment.
- The following Python packages installed:
  - `mysql-connector-python`
  - `pandas`
  - `requests`
  - `logging`
  - `pydrive`
- Apache Airflow installed.
- A MySQL database set up or any other database.

## Repository Structure

- The ['notebook'](./notebook) folder contains the 'eda.ipynb' file, which includes the Exploratory Data Analysis (EDA) for both the Spotify and Grammy data.

- Inside the ['my_dags'](./my_dags) folder, you'll find the code to run the Apache Airflow workflow for the ETL process.

- The ['visualizations'](./visualizations) folder houses the final charts and visualizations generated from the resulting 'songs.csv' file, which is also located within thhis same folder.
