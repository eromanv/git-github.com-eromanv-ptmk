from database import create_db_engine, create_db_session
from sqlalchemy import Column, String, Date, Enum, Integer, Index
from sqlalchemy.orm import declarative_base
from datetime import datetime
import random
import time
import sys


Base = declarative_base()


class Employee(Base):
    __tablename__ = "employees"
    id = Column(Integer, primary_key=True)
    full_name = Column(String)
    birth_date = Column(Date)
    gender = Column(Enum("Male", "Female", name="gender"))


class EmployeeRepository:
    def __init__(self, session):
        self.session = session

    def create_table(self):
        Base.metadata.create_all(self.session.get_bind())

    def add_employee(self, full_name, birth_date, gender):
        employee = Employee(
            full_name=full_name,
            birth_date=datetime.strptime(birth_date, "%Y-%m-%d"),
            gender=gender,
        )

        self.session.add(employee)
        self.session.commit()

    def display_employees(self):
        employees = (
            self.session.query(Employee).order_by(Employee.full_name).all()
        )

        for employee in employees:
            age = self.calculate_age(employee.birth_date)
            print(
                f"{employee.full_name}, {employee.birth_date}, {employee.gender}, {age} years old"
            )

    def calculate_age(self, birth_date):
        today = datetime.today()
        age = (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )
        return age

    def batch_insert(self, data):
        employees = [
            Employee(
                full_name=name,
                birth_date=datetime.strptime(bd, "%Y-%m-%d"),
                gender=gender,
            )
            for name, bd, gender in data
        ]

        self.session.bulk_save_objects(employees)
        self.session.commit()

    def query_and_measure_time(self):
        start_time = time.time()
        results = (
            self.session.query(Employee)
            .filter(Employee.full_name.like("F%"), Employee.gender == "Male")
            .all()
        )
        end_time = time.time()

        for result in results:
            print(result.full_name, result.birth_date, result.gender)

        print(f"time: {end_time - start_time} seconds")


class DataGenerator:
    @staticmethod
    def generate_random_data(count=1000000):
        names = ["Ivan", "Dmitry", "Alexei"]
        last_names = ["Ivanov", "Fedorov", "Sidorov", "Popov"]

        genders = ["Male", "Female"]
        data = []

        for _ in range(count):
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

    @staticmethod
    def generate_specific_data(count=100):
        names = ["Ivan", "Dmitry", "Alexei"]
        last_names = ["Ivanov", "Fedorov", "Sidorov", "Popov"]

        data = []

        for _ in range(count):
            full_name = (
                "F"
                + random.choice(names)
                + " "
                + random.choice(last_names)
                + " "
                + random.choice(last_names)
            )
            birth_date = f"{random.randint(1950, 2000)}-{random.randint(1, 12)}-{random.randint(1, 28)}"
            gender = "Male"
            data.append((full_name, birth_date, gender))

        return data

    @staticmethod
    def generate_data(random_count=1000000, specific_count=100):
        random_data = DataGenerator.generate_random_data(random_count)
        specific_data = DataGenerator.generate_specific_data(specific_count)
        return random_data + specific_data


if __name__ == "__main__":
    mode = int(sys.argv[1])

    engine = create_db_engine("postgres", "postgres", "ptmk")
    session = create_db_session(engine)

    employee_repository = EmployeeRepository(session)
    data_generator = DataGenerator()

    if mode == 1:
        employee_repository.create_table()

    elif mode == 2:
        full_name = sys.argv[2]
        birth_date = sys.argv[3]
        gender = sys.argv[4]
        employee_repository.add_employee(full_name, birth_date, gender)

    elif mode == 3:
        employee_repository.display_employees()

    elif mode == 4:
        data = data_generator.generate_data()
        employee_repository.batch_insert(data)

    elif mode == 5:
        print("\n query without indexes:")
        employee_repository.query_and_measure_time()

        # Добавляем индексы
        Index("idx_gender", Employee.gender)
        Index("idx_full_name", Employee.full_name)

        # Выполняем запрос с индексами
        print("\n query with indexes:")
        employee_repository.query_and_measure_time()
