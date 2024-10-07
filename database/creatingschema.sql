CREATE TABLE Students (
    student_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department_id INT,
    email VARCHAR(100)
);



CREATE TABLE Courses (
    course_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department_id INT,
    credits INT
);



CREATE TABLE Enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INT REFERENCES Students(student_id),
    course_id INT REFERENCES Courses(course_id),
    grade CHAR(1),
    enrollment_date DATE
);


CREATE TABLE Instructors (
    instructor_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department_id INT,
    email VARCHAR(100)
);
CREATE TABLE Departments (
    department_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);
