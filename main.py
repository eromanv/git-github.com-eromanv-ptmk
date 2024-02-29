import sys
from database import create_db_engine, create_db_session
from sqlalchemy import Column, String, Date, Enum, Integer
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import random
import time

Base = declarative_base()


class Employee(Base):
    __tablename__ = "employees"
    id = Column(Integer, primary_key=True)
    full_name = Column(String)
    birth_date = Column(Date)
    gender = Column(Enum("Male", "Female"))


def create_table(session):
    Base.metadata.create_all(session.get_bind())


def add_employee(session, full_name, birth_date, gender):
    employee = Employee(
        full_name=full_name,
        birth_date=datetime.strptime(birth_date, "%Y-%m-%d"),
        gender=gender,
    )

    session.add(employee)
    session.commit()


def display_employees(session):
    employees = session.query(Employee).order_by(Employee.full_name).all()

    for employee in employees:
        age = calculate_age(employee.birth_date)
        print(
            f"{employee.full_name}, {employee.birth_date}, {employee.gender}, {age} years old"
        )


def calculate_age(birth_date):
    today = datetime.today()
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


def generate_random_data():
    names = ["John", "Jane", "James", "Jennifer", "Jack", "Jessica"]
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis"]
    genders = ["Male", "Female"]
    data = []

    for _ in range(1000000):
        full_name = (
            random.choice(names)
            + " "
            + random.choice(last_names)
            + " "
            + random.choice(last_names)
        )
        birth_date = f"{random.randint(1950, 2000)}-{random.randint(1, 12)}-{random.randint(1, 28)}"
        gender = random.choice(genders)
        data.append((full_name, birth_date, gender))

    return data


def batch_insert(session, data):
    employees = [
        Employee(
            full_name=name, birth_date=datetime.strptime(bd, "%Y-%m-%d"), gender=gender
        )
        for name, bd, gender in data
    ]

    session.bulk_save_objects(employees)
    session.commit()


def query_and_measure_time(session):
    start_time = time.time()
    results = (
        session.query(Employee)
        .filter(Employee.gender == "Male", Employee.full_name.like("F%"))
        .all()
    )
    end_time = time.time()

    for result in results:
        print(result.full_name, result.birth_date, result.gender)

    print(f"Execution time: {end_time - start_time} seconds")


if __name__ == "__main__":
    mode = int(sys.argv[1])

    engine = create_db_engine("your_username", "your_password", "your_database")
    session = create_db_session(engine)

    if mode == 1:
        create_table(session)

    elif mode == 2:
        full_name = sys.argv[2]
        birth_date = sys.argv[3]
        gender = sys.argv[4]
        add_employee(session, full_name, birth_date, gender)

    elif mode == 3:
        display_employees(session)

    elif mode == 4:
        data = generate_random_data()
        batch_insert(session, data)

    elif mode == 5:
        query_and_measure_time(session)
