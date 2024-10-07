

# import pyspark
# from pyspark.sql import SparkSession
# import os

# if __name__ == "__main__":
#     # Build the absolute paths to the JAR files
#     jar_files_list = [
#         "mongo-spark-connector_2.12-10.1.1.jar",
#         "mongodb-driver-sync-4.8.2.jar",
#         "mongodb-driver-core-4.8.2.jar",
#         "bson-4.8.2.jar",
#         "bson-record-codec-4.8.2.jar"
#     ]

#     # Use ';' as the classpath separator on Windows
#     classpath = ";".join(jar_files_list)

#     # Set the PYSPARK_SUBMIT_ARGS environment variable
#     os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-class-path \"{classpath}\" --jars \"{classpath}\" pyspark-shell"

#     # Initialize Spark Session without spark.jars
#     spark = SparkSession \
#         .builder \
#         .appName("MongoDBIntegration") \
#         .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/University") \
#         .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/University") \
#         .getOrCreate()

#     # Read from MongoDB
#     readStudents = spark.read \
#         .format("mongodb") \
#         .option("database", "University") \
#         .option("collection", "Students") \
#         .load()

#     # Print the schema to verify
#     readStudents.printSchema()

#     # Create a temporary view to query the data using SQL
#     readStudents.createOrReplaceTempView("Students")

#     # Execute an SQL query
#     sqlDF = spark.sql("SELECT name FROM Students")

#     # Show the query result
#     sqlDF.show()

#     # Unpersist DataFrames
#     readStudents.unpersist()
#     sqlDF.unpersist()

#     # Stop the Spark session
#     spark.stop()



import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, countDistinct, avg, desc, array_intersect
from pyspark.sql.types import StringType, ArrayType
import os

if __name__ == "__main__":
    # Build the absolute paths to the JAR files
    jar_files_list = [
        "mongo-spark-connector_2.12-10.1.1.jar",
        "mongodb-driver-sync-4.8.2.jar",
        "mongodb-driver-core-4.8.2.jar",
        "bson-4.8.2.jar",
        "bson-record-codec-4.8.2.jar"
    ]

    # Use ';' as the classpath separator on Windows or ':' on Unix/Linux
    classpath = ";".join(jar_files_list) if os.name == 'nt' else ":".join(jar_files_list)

    # Set the PYSPARK_SUBMIT_ARGS environment variable
    os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-class-path \"{classpath}\" --jars \"{classpath}\" pyspark-shell"

    # Initialize Spark Session without spark.jars
    spark = SparkSession \
        .builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/University") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/University") \
        .getOrCreate()

    # Read collections from MongoDB
    studentsDF = spark.read \
        .format("mongodb") \
        .option("database", "University") \
        .option("collection", "Students") \
        .load()

    coursesDF = spark.read \
        .format("mongodb") \
        .option("database", "University") \
        .option("collection", "Courses") \
        .load()

    instructorsDF = spark.read \
        .format("mongodb") \
        .option("database", "University") \
        .option("collection", "Instructors") \
        .load()

    departmentsDF = spark.read \
        .format("mongodb") \
        .option("database", "University") \
        .option("collection", "Departments") \
        .load()

    # 1. Fetching all students enrolled in a specific course (e.g., '1')
    course_id = 1  # Replace with the actual course ID
    # Explode the 'enrollments' array in students
    enrollmentsDF = studentsDF.select(
        col('_id').alias('student_id'),
        'name',
        'department',
        explode('enrollments').alias('enrollment')
    )
    
    # Filter for the specific course
    students_in_course = enrollmentsDF.filter(col('enrollment.course_id') == course_id)
    print(f"Students enrolled in course {course_id}:")
    students_in_course.select('student_id', 'name', 'department').show()

    # 2. Calculating the average number of students enrolled in courses offered by a particular instructor (e.g., '1')
    instructor_id = 1  # Replace with the actual instructor ID
    # Get the list of courses taught by the instructor
    instructor_courses = instructorsDF.filter(col('_id') == instructor_id).select('courses_taught').collect()
    if instructor_courses:
        courses_taught = instructor_courses[0]['courses_taught']
        # Filter coursesDF for these courses
        courses_taughtDF = coursesDF.filter(col('_id').isin(courses_taught))
        # Calculate the number of students enrolled in each course
        courses_enrollment = courses_taughtDF.select(
            '_id',
            'name',
            size('enrolled_students').alias('num_students')
        )
        # Calculate the average number of students
        average_students = courses_enrollment.select(avg('num_students').alias('average_enrollment'))
        print(f"Average number of students in courses taught by instructor {instructor_id}:")
        average_students.show()
    else:
        print(f"No courses found for instructor {instructor_id}.")

    # 3. Listing all courses offered by a specific department (e.g., 'Computer Science')
    department_name = 'Computer Science'  # Replace with the actual department name
    courses_in_department = coursesDF.filter(col('department') == department_name)
    print(f"Courses offered by department {department_name}:")
    courses_in_department.select('_id', 'name', 'credit').show()

    # 4. Finding the total number of students per department
    # Explode 'enrolled_students' in courses
    courses_studentsDF = coursesDF.select(
        'department',
        explode('enrolled_students').alias('student_id')
    )
    # Group by department and count distinct students
    students_per_department = courses_studentsDF.groupBy('department') \
        .agg(countDistinct('student_id').alias('total_students'))
    print("Total number of students per department:")
    students_per_department.show()

    # 5. Finding instructors who have taught all the BTech CSE core courses
    # Assuming we have a list of core course IDs
    core_course_ids = [1, 2, 3]  # Replace with actual core course IDs
    total_core_courses = len(core_course_ids)
    # Create a DataFrame with core courses
    core_coursesDF = spark.createDataFrame([(core_course_ids,)], ['core_course_ids'])
    # Cross join instructors with core courses
    instructors_with_core = instructorsDF.crossJoin(core_coursesDF)
    # Compute intersection of courses_taught and core_course_ids
    instructors_with_core = instructors_with_core.withColumn(
        'taught_core_courses',
        array_intersect('courses_taught', 'core_course_ids')
    )
    # Count the number of core courses taught
    instructors_with_core = instructors_with_core.withColumn(
        'num_core_courses_taught',
        size('taught_core_courses')
    )
    # Filter instructors who have taught all core courses
    instructors_who_taught_all_core = instructors_with_core.filter(
        col('num_core_courses_taught') == total_core_courses
    )
    print("Instructors who have taught all BTech CSE core courses:")
    instructors_who_taught_all_core.select('_id', 'name').show()

    # 6. Finding top-10 courses with the highest enrollments
    courses_with_enrollments = coursesDF.select(
        '_id',
        'name',
        size('enrolled_students').alias('num_enrollments')
    )
    top_10_courses = courses_with_enrollments.orderBy(desc('num_enrollments')).limit(10)
    print("Top 10 courses with the highest enrollments:")
    top_10_courses.show()

    # Stop the Spark session
    spark.stop()
