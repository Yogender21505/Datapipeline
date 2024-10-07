import psycopg2
from pymongo import MongoClient
from datetime import date

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect("dbname=postgres user=postgres password=admin host=localhost port=5433")
    cursor = conn.cursor()

    # Initialize maps that will transform relationships efficiently
    students_map = {}       # student_id -> student record (with embedded enrollments)
    courses_map = {}        # course_id -> course record (with embedded students and instructors)
    instructors_map = {}    # instructor_id -> instructor record
    departments_map = {}    # department_id -> department record
    enrollments_list = []   # List to hold enrollment documents for MongoDB

    # 1. Fetch Departments and create a direct Department map
    cursor.execute("SELECT * FROM Departments")
    departments_data = cursor.fetchall()
    departments_map = {
        department[0]: {
            "_id": department[0],        # "department_id"
            "name": department[1]        # "department_name"
        }
        for department in departments_data
    }

    # 2. Insert Departments into MongoDB early (we can use the department names later)
    client = MongoClient("mongodb://localhost:27017/")
    db = client["University"]
    db["Departments"].insert_many(list(departments_map.values()))  # Insert departments into MongoDB

    # 3. Fetch and Transform Students data (with embedded enrollments)
    cursor.execute("SELECT * FROM Students")
    students_data = cursor.fetchall()

    # Create an initial student record map
    for student in students_data:
        student_id = student[0]
        students_map[student_id] = {
            "_id": student_id,
            "name": student[1],  # student_name
            "department": departments_map.get(student[2], {}).get("name"),  # dept_name
            "enrollments": []    # Will be populated with enrolled course info
        }

    # 4. Fetch and Transform Courses data (we’ll populate with students and instructors later)
    cursor.execute("SELECT * FROM Courses")
    courses_data = cursor.fetchall()

    # Create a course map (initially empty instructors and enrolled_students)
    for course in courses_data:
        course_id = course[0]
        courses_map[course_id] = {
            "_id": course_id,
            "name": course[1],  # course_name
            "department": departments_map.get(course[2], {}).get("name"),  # department_name
            "credit": course[3],  # credit hours
            "instructors": [],    # We'll populate with instructor names
            "enrolled_students": []  # We'll populate with student IDs
        }

    # 5. Fetch and process Enrollments (to link students and courses together)
    cursor.execute("SELECT * FROM Enrollments")
    enrollments_data = cursor.fetchall()

    for enr in enrollments_data:
        enrollment_id, student_id, course_id, grade, enrollment_date = enr

        # Convert the date to a string
        enrollment_date = enrollment_date.isoformat() if isinstance(enrollment_date, date) else enrollment_date

        # Append enrollment info to students
        students_map[student_id]["enrollments"].append({
            "course_id": course_id,
            "course_name": courses_map[course_id]["name"],  # Resolve course name from courses_map
            "grade": grade,
            "enrollment_date": enrollment_date
        })

        # Append student info to the respective course's 'enrolled_students'
        courses_map[course_id]["enrolled_students"].append(student_id)

        # Create an enrollment document and append to enrollments_list
        enrollment_doc = {
            "_id": enrollment_id,
            "student_id": student_id,
            "student_name": students_map[student_id]["name"],
            "course_id": course_id,
            "course_name": courses_map[course_id]["name"],
            "grade": grade,
            "enrollment_date": enrollment_date
        }
        enrollments_list.append(enrollment_doc)

    # 6. Fetch Instructors and associate them with courses
    cursor.execute("SELECT * FROM Instructors")
    instructors_data = cursor.fetchall()

    for instructor in instructors_data:
        instructor_id = instructor[0]
        instructor_name = instructor[1]
        dept_id = instructor[2]
        email = instructor[3]

        # Create an instructor record in instructors_map
        instructors_map[instructor_id] = {
            "_id": instructor_id,
            "name": instructor_name,      # instructor_name
            "department": departments_map.get(dept_id, {}).get("name"),  # instructor's department name
            "email": email, 
            "courses_taught": []  # Will be populated with course IDs
        }

        # Associate instructors with specific courses via department
        for course_id, course in courses_map.items():
            # If this instructor belongs to the same department as the course
            if course["department"] == instructors_map[instructor_id]["department"]:
                # Add this instructor to the course’s 'instructors' field
                courses_map[course_id]["instructors"].append(instructor_name)
                # Add this course to the instructor’s 'courses_taught' field
                instructors_map[instructor_id]["courses_taught"].append(course_id)

    # Insert transformed data into MongoDB

    # Insert Students (with embedded enrollments)
    db["Students"].insert_many(list(students_map.values()))

    # Insert Courses (with embedded students and instructors)
    db["Courses"].insert_many(list(courses_map.values()))

    # Insert Instructors (with courses they teach)
    db["Instructors"].insert_many(list(instructors_map.values()))

    # Insert Enrollments (separate collection)
    db["Enrollments"].insert_many(enrollments_list)

    print("Data successfully transferred to MongoDB!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if client:
        client.close()
