import psycopg2
import csv

# PostgreSQL connection details
conn = psycopg2.connect("dbname=postgres user=postgres password=admin host=localhost port=5433")

try:
    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Helper function to load CSV data into a table
    def load_csv_into_table(csv_file, table_name, columns):
        with open(csv_file, mode='r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header line
            for row in reader:
                cursor.execute(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})", row
                )
        conn.commit()

    # Load Departments CSV
    load_csv_into_table("./database/Departments.csv", "Departments", ["department_id", "name"])

    # Load Students CSV
    load_csv_into_table("./database/Students.csv", "Students", ["student_id", "name", "department_id", "email"])

    # Load Courses CSV
    load_csv_into_table("./database/Courses.csv", "Courses", ["course_id", "name", "department_id", "credits"])

    # Load Instructors CSV
    load_csv_into_table("./database/Instructors.csv", "Instructors", ["instructor_id", "name", "department_id", "email"])

    # Load Enrollments CSV
    load_csv_into_table("./database/Enrollments.csv", "Enrollments", ["enrollment_id", "student_id", "course_id", "grade", "enrollment_date"])

    print("Data successfully inserted into PostgreSQL!")

except Exception as e:
    print(f"An error occurred while inserting data: {e}")

finally:
    cursor.close()
    conn.close()