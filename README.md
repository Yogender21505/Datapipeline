# University Data Pipeline and Query Optimization

_Apache Spark • MongoDB • PostgreSQL • Python_

This project demonstrates an end-to-end data pipeline using **Apache Spark**, **MongoDB**, and **PostgreSQL**. It includes data modeling, ETL processes for data migration from a Relational Database (PostgreSQL) to a NoSQL (MongoDB) schema, and query optimizations for efficient data retrieval, using Spark for distributed analytics and MongoDB for document storage.

## Table of Contents
- [Project Objective](#project-objective)
- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [Installation](#installation)
- [Data Pipeline](#data-pipeline)
- [Queries Implemented](#queries-implemented)
- [Performance Optimization](#performance-optimization)
- [Run the Project](#run-the-project)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)

## Project Objective

The goal of this project is to build a **scalable data pipeline** capable of:  
1. **Migrating data** from relational databases to MongoDB in a well-optimized format for querying.
2. **Optimizing the schema** to enable efficient data analysis using Apache Spark.
3. **Performing distributed queries** to answer complex questions about university-related data, including student enrollments, average enrollments per instructor, and top courses by size.

## Prerequisites

Before you begin, ensure you have the following installed:
- **Java** (version 8 or greater)
- **Python** (version 3.x or greater)
- **PostgreSQL** (version 14.x) - [Download Here](https://www.postgresql.org/download/)
- **MongoDB** (version 8.x) - [Download Here](https://www.mongodb.com/try/download/community)
- **Apache Spark** (version 3.4.3) - [Download Here](https://spark.apache.org/downloads.html)
- MongoDB Spark Connector JAR files:
  - [mongo-spark-connector_2.12-10.1.1.jar](https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/)
  - [mongodb-driver-sync-4.8.2.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/)
  - [mongodb-driver-core-4.8.2.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/)
  - [bson-4.8.2.jar](https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/)
  - [bson-record-codec-4.8.2.jar](https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/)

Ensure you set the paths of your installed tools in the system environment variables.

## Architecture Overview

The project involves two main database technologies — **PostgreSQL** (RDBMS) and **MongoDB** (NoSQL) and uses **Apache Spark** for executing distributed queries.

1. **PostgreSQL**: Relational schema design with entities like Students, Courses, Instructors, and Departments, holding normalized data.
2. **MongoDB**: Denormalized schema design optimized for efficient querying and retrieval of complex nested relationships without frequent joins.
3. **Apache Spark**: Used for distributed computation, Spark executes high-performance queries across MongoDB collections.

## Screenshots
## 1. Schema
<img src="https://github.com/user-attachments/assets/30e6fdab-c17f-40c1-829a-789a082a863b" width="600" alt="Schema">

## 2. MongoDB Optimized Schema

Courses

<img src="https://github.com/user-attachments/assets/62348831-e7b5-4e26-9e03-6c7c228ce2eb" width="600" alt="Courses Schema">

Departments

<img src="https://github.com/user-attachments/assets/a1476778-d356-4498-9d5f-2138f236c348" width="600" alt="Departments Schema">

Enrollments

<img src="https://github.com/user-attachments/assets/17293136-9573-45dc-81b8-b2c1d3f523fd" width="600" alt="Enrollments Schema">

Instructors

<img src="https://github.com/user-attachments/assets/2de58945-b5f7-4f2d-b636-bae86b8b947e" width="600" alt="Instructors Schema">

Students

<img src="https://github.com/user-attachments/assets/9d8aa31a-771a-470b-abd4-c5f8840c9104" width="600" alt="Students Schema">

## 3. Evaluation

<img src="https://github.com/user-attachments/assets/962d465b-0527-4015-b3ea-1ae785843a6a" width="600" alt="Evaluation">

## Installation

1. Download and install **PostgreSQL**, **MongoDB**, and **Apache Spark** from their respective websites or package managers.
2. Download the required MongoDB connector JAR files (listed in prerequisites).
3. Set up each tool's path in the **System Environment Variables**.
4. Clone this repository and navigate to the project directory:

```bash
git clone https://github.com/Yogender21505/Datapipeline.git
cd Datapipeline
```

5. Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

## Data Pipeline

### Relational Database Schema in PostgreSQL:
The schema consists of the following normalized tables:

- Students: Contains fields like student ID, name, and department.
- Courses: Contains course information such as course ID, name, and credits.
- Instructors, Departments, and Enrollments tables manage relationships between entities.

### Denormalized Schema in MongoDB:
Data is redesigned for MongoDB to minimize joins and optimize performance:

- Embedded course enrollment details directly within each student document.
- Courses only store student IDs to reduce redundancy.
- Instructors directly embed course information to minimize joins.

### ETL Process:
The ETL (Extract, Transform, Load) process migrates data from PostgreSQL to MongoDB.

1. Extract: Run SQL queries to extract data from PostgreSQL.
2. Transform: Reformat the extracted data to fit the denormalized MongoDB schema.
3. Load: Insert the transformed data into MongoDB collections.

Steps to execute the migration:

```bash
# Load CSV data into PostgreSQL tables
python ./importdatatopostgres.py

# Migrate data from PostgreSQL to MongoDB
python ./Rdbmstomongoconnector.py
```

## Queries Implemented

Queries are executed on MongoDB using Apache Spark for distributed processing:

1. Fetch students enrolled in a specific course.
2. Calculate the average number of students per instructor.
3. List all courses offered by a specific department.
4. Find the total number of students per department.
5. Identify instructors who have taught all core courses.
6. List the top-10 courses with the highest enrollments.

## Performance Optimization

### MongoDB Indexing
Indexes were created on fields like `course_id`, `department`, and `courses_taught` to boost performance by 60%.

### Apache Spark Optimization
- DataFrame caching was implemented to avoid recomputation, improving query runtime for frequently executed queries by 50%.
- Optimized aggregation queries using array functions such as `array_intersect` for faster array operations.

## Run the Project

1. Start the PostgreSQL and MongoDB servers.

2. Run the ETL scripts:

```bash
python ./importdatatopostgres.py
python ./Rdbmstomongoconnector.py
```

3. Launch the Spark session and execute queries:

```bash
spark-submit main.py
```

This will trigger the Spark job, read data from MongoDB collections, and execute the queries.

## Technologies Used

- PostgreSQL: Relational Database Management System (RDBMS)
- MongoDB: NoSQL Document Database
- Apache Spark: Distributed Data Processing Framework
- Python: Backend scripting and ETL implementation
- JDBC/PyMongo: Database connectors for PostgreSQL and MongoDB
- JSON/CSV: Intermediate data formats used for migration

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b new-feature`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin new-feature`).
5. Create a Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE).