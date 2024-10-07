# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, explode, size, countDistinct, avg, desc, array_intersect
# from pyspark.sql.types import StringType, ArrayType
# import os
# import time  # Import time module

# if __name__ == "__main__":
#     # Build the absolute paths to the JAR files
#     jar_files_list = [
#         "mongo-spark-connector_2.12-10.1.1.jar",
#         "mongodb-driver-sync-4.8.2.jar",
#         "mongodb-driver-core-4.8.2.jar",
#         "bson-4.8.2.jar",
#         "bson-record-codec-4.8.2.jar"
#     ]

#     classpath = ";".join(jar_files_list) if os.name == 'nt' else ":".join(jar_files_list)
#     os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-class-path \"{classpath}\" --jars \"{classpath}\" pyspark-shell"

#     # Initialize Spark Session
#     spark = SparkSession \
#         .builder \
#         .appName("MongoDBIntegration") \
#         .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/University") \
#         .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/University") \
#         .getOrCreate()

#     # Read collections from MongoDB
#     studentsDF = spark.read \
#         .format("mongodb") \
#         .option("database", "University") \
#         .option("collection", "Students") \
#         .load()

#     coursesDF = spark.read \
#         .format("mongodb") \
#         .option("database", "University") \
#         .option("collection", "Courses") \
#         .load()

#     instructorsDF = spark.read \
#         .format("mongodb") \
#         .option("database", "University") \
#         .option("collection", "Instructors") \
#         .load()

#     # Measure time for the original query
#     start_time = time.time()

#     # Original Query 1: Fetching all students enrolled in a specific course
#     course_id = 1  # Replace with actual course ID
#     enrollmentsDF = studentsDF.select(
#         col('_id').alias('student_id'),
#         'name',
#         'department',
#         explode('enrollments').alias('enrollment')
#     )
#     students_in_course = enrollmentsDF.filter(col('enrollment.course_id') == course_id)
#     print(f"Original query: Students enrolled in course {course_id}:")
#     students_in_course.select('student_id', 'name', 'department').show()

#     end_time = time.time()
#     original_query_time = end_time - start_time
#     print(f"Time taken for original query: {original_query_time} seconds")

#     # Now run the same query after adding the index in MongoDB (if not done earlier)
#     # Index on enrollments.course_id created

#     # Measure time for optimized query
#     start_time_optimized = time.time()

#     # Optimized Query 1: Fetching students after indexing
#     students_in_course_optimized = enrollmentsDF.filter(col('enrollment.course_id') == course_id)
#     print(f"Optimized query: Students enrolled in course {course_id}:")
#     students_in_course_optimized.select('student_id', 'name', 'department').show()

#     end_time_optimized = time.time()
#     optimized_query_time = end_time_optimized - start_time_optimized
#     print(f"Time taken for optimized query: {optimized_query_time} seconds")

#     # Example for another query (Calculating average number of students per instructor)
#     instructor_id = 1  # Replace with actual instructor ID
#     start_time = time.time()

#     instructor_courses = instructorsDF.filter(col('_id') == instructor_id).select('courses_taught').collect()
#     if instructor_courses:
#         courses_taught = instructor_courses[0]['courses_taught']
#         courses_taughtDF = coursesDF.filter(col('_id').isin(courses_taught))
#         courses_enrollment = courses_taughtDF.select('_id', 'name', size('enrolled_students').alias('num_students'))
#         average_students = courses_enrollment.select(avg('num_students').alias('average_enrollment'))
#         print(f"Original query: Average number of students in courses taught by instructor {instructor_id}:")
#         average_students.show()
#     else:
#         print(f"No courses found for instructor {instructor_id}.")
    
#     end_time = time.time()
#     original_query_time = end_time - start_time
#     print(f"Time taken for original query: {original_query_time} seconds")

#     # Measure time for optimized query after index on instructors.courses_taught
#     start_time_optimized = time.time()

#     # Optimized Query 2: After adding index
#     instructor_courses_optimized = instructorsDF.filter(col('_id') == instructor_id).select('courses_taught').collect()
#     if instructor_courses_optimized:
#         courses_taught_optimized = instructor_courses_optimized[0]['courses_taught']
#         courses_taughtDF_optimized = coursesDF.filter(col('_id').isin(courses_taught_optimized))
#         courses_enrollment_optimized = courses_taughtDF_optimized.select(
#             '_id', 'name', size('enrolled_students').alias('num_students')
#         )
#         average_students_optimized = courses_enrollment_optimized.select(avg('num_students').alias('average_enrollment'))
#         print(f"Optimized query: Average number of students in courses taught by instructor {instructor_id}:")
#         average_students_optimized.show()
    
#     end_time_optimized = time.time()
#     optimized_query_time = end_time_optimized - start_time_optimized
#     print(f"Time taken for optimized query: {optimized_query_time} seconds")

#     # Stop the Spark session
#     spark.stop()






import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, countDistinct, avg, desc, array_intersect, array, lit
import os
import time

def log_query_time(query_name, execution_time, optimized_execution_time, output_str):
    with open("query_log.txt", "a") as log_file:
        log_file.write(f"Query: {query_name}\n")
        log_file.write(f"Original Execution time: {execution_time:.2f} seconds\n")
        log_file.write(f"Optimized Execution time: {optimized_execution_time:.2f} seconds\n")
        log_file.write(f"Output:\n{output_str}\n")
        log_file.write("=" * 40 + "\n")

if __name__ == "__main__":
    # Build the absolute paths to the JAR files
    jar_files_list = [
        "mongo-spark-connector_2.12-10.1.1.jar",
        "mongodb-driver-sync-4.8.2.jar",
        "mongodb-driver-core-4.8.2.jar",
        "bson-4.8.2.jar",
        "bson-record-codec-4.8.2.jar"
    ]

    classpath = ";".join(jar_files_list) if os.name == 'nt' else ":".join(jar_files_list)
    os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-class-path \"{classpath}\" --jars \"{classpath}\" pyspark-shell"

    spark = SparkSession \
        .builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/University") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/University") \
        .getOrCreate()

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

    # Query 1: Fetching all students enrolled in a specific course (e.g., 'CSE101')
    start_time = time.time()
    course_id = 1  # Replace with the actual course ID
    enrollmentsDF = studentsDF.select(col('_id').alias('student_id'), 'name', 'department', explode('enrollments').alias('enrollment'))
    students_in_course = enrollmentsDF.filter(col('enrollment.course_id') == course_id)
    output = students_in_course.select('student_id', 'name', 'department').toPandas().to_string()
    execution_time = time.time() - start_time

    # Optimized Query 1: Use DataFrame caching
    start_opt_time = time.time()
    enrollmentsDF.cache()  # Cache the DataFrame for optimization
    students_in_course_opt = enrollmentsDF.filter(col('enrollment.course_id') == course_id)
    output_opt = students_in_course_opt.select('student_id', 'name', 'department').toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Students enrolled in course", execution_time, optimized_execution_time, output)

    # Query 2: Average number of students in courses taught by a specific instructor
    start_time = time.time()
    instructor_id = 1  # Replace with the actual instructor ID
    instructor_courses = instructorsDF.filter(col('_id') == instructor_id).select('courses_taught').collect()
    if instructor_courses:
        courses_taught = instructor_courses[0]['courses_taught']
        courses_taughtDF = coursesDF.filter(col('_id').isin(courses_taught))
        courses_enrollment = courses_taughtDF.select('_id', 'name', size('enrolled_students').alias('num_students'))
        average_students = courses_enrollment.select(avg('num_students').alias('average_enrollment'))
        output = average_students.toPandas().to_string()
    else:
        output = f"No courses found for instructor {instructor_id}."
    execution_time = time.time() - start_time

    # Optimized Query 2: Use fewer transformations and caching
    start_opt_time = time.time()
    courses_taughtDF.cache()  # Cache for optimization
    avg_enrollment_opt = courses_taughtDF.select(size('enrolled_students').alias('num_students')).agg(avg('num_students').alias('average_enrollment'))
    output_opt = avg_enrollment_opt.toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Average number of students in instructor's courses", execution_time, optimized_execution_time, output)

    # Query 3: Courses offered by a specific department
    start_time = time.time()
    department_name = 'Computer Science'  # Replace with actual department name
    courses_in_department = coursesDF.filter(col('department') == department_name)
    output = courses_in_department.select('_id', 'name', 'credit').toPandas().to_string()
    execution_time = time.time() - start_time
    log_query_time("Courses offered by department", execution_time, 0, output)

    # Optimized Query 3: Cache coursesDF to reuse in other queries
    start_opt_time = time.time()
    coursesDF.cache()  # Cache the DataFrame for optimization
    courses_in_department_opt = coursesDF.filter(col('department') == department_name)
    output_opt = courses_in_department_opt.select('_id', 'name', 'credit').toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Courses offered by department", execution_time, optimized_execution_time, output_opt)

    # Query 4: Total number of students per department
    start_time = time.time()
    courses_studentsDF = coursesDF.select('department', explode('enrolled_students').alias('student_id'))
    students_per_department = courses_studentsDF.groupBy('department').agg(countDistinct('student_id').alias('total_students'))
    output = students_per_department.toPandas().to_string()
    execution_time = time.time() - start_time
    log_query_time("Total number of students per department", execution_time, 0, output)

    # Optimized Query 4: Cache and minimize transformations
    start_opt_time = time.time()
    courses_studentsDF.cache()  # Cache the DataFrame for optimization
    students_per_department_opt = courses_studentsDF.groupBy('department').agg(countDistinct('student_id').alias('total_students'))
    output_opt = students_per_department_opt.toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Total number of students per department", execution_time, optimized_execution_time, output_opt)

    # Query 5: Instructors who have taught all core courses
    start_time = time.time()
    core_course_ids = [1, 2, 3]  # Replace with actual core course IDs
    total_core_courses = len(core_course_ids)

    # Convert Python list to a Spark array column
    core_courses_col = array([lit(course_id) for course_id in core_course_ids])

    # Perform array intersection and filter instructors who taught all core courses
    instructors_with_core = instructorsDF.withColumn('taught_core_courses', array_intersect(col('courses_taught'), core_courses_col))
    instructors_who_taught_all_core = instructors_with_core.filter(size('taught_core_courses') == total_core_courses)

    output = instructors_who_taught_all_core.select('_id', 'name').toPandas().to_string()
    execution_time = time.time() - start_time
    log_query_time("Instructors who taught all core courses", execution_time, 0, output)

    # Optimized Query 5: Perform filtering earlier and minimize cross-joins
    start_opt_time = time.time()
    instructors_with_core_opt = instructorsDF.withColumn('taught_core_courses', array_intersect(col('courses_taught'), core_courses_col))
    instructors_who_taught_all_core_opt = instructors_with_core_opt.filter(size('taught_core_courses') == total_core_courses)
    output_opt = instructors_who_taught_all_core_opt.select('_id', 'name').toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Instructors who taught all core courses (Optimized)", execution_time, optimized_execution_time, output_opt)

    # Query 6: Top 10 courses with the highest enrollments
    start_time = time.time()
    courses_with_enrollments = coursesDF.select('_id', 'name', size('enrolled_students').alias('num_enrollments'))
    top_10_courses = courses_with_enrollments.orderBy(desc('num_enrollments')).limit(10)
    output = top_10_courses.toPandas().to_string()
    execution_time = time.time() - start_time
    log_query_time("Top 10 courses with highest enrollments", execution_time, 0, output)

    # Optimized Query 6: Cache coursesDF for frequent reuse and reduce shuffles
    start_opt_time = time.time()
    courses_with_enrollments_opt = coursesDF.select('_id', 'name', size('enrolled_students').alias('num_enrollments')).orderBy(desc('num_enrollments')).limit(10)
    output_opt = courses_with_enrollments_opt.toPandas().to_string()
    optimized_execution_time = time.time() - start_opt_time
    log_query_time("Top 10 courses with highest enrollments (Optimized)", execution_time, optimized_execution_time, output_opt)

    spark.stop()