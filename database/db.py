import enum
import uuid
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Enum, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from .storage import minio_client

"""
Database configuration and SQLAlchemy models for the MapReduce platform.
This module establishes the connection to the PostgreSQL database (DDS)
and defines the Object-Relational Mapping (ORM) schema for Jobs and Tasks.
It allows the UI and Manager microservices to interact with the database 
state using Python classes instead of raw SQL queries.

It provides a `get_db()` dependency generator to safely yield 
database sessions for the microservice endpoints.

It also configures the MinIO object storage connection and
provides a `get_minio()` dependency generator for the endpoints.


"""
import os

SQLALCHEMY_DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://mapreduce_user:mapreduce_password@postgres:5432/mapreduce_db")

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class JobStatus(enum.Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskType(enum.Enum):
    MAP = "MAP"
    REDUCE = "REDUCE"

class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String(255), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.SUBMITTED)
    input_code_ref = Column(String(512), nullable=False)
    mapper_code_ref = Column(String(512), nullable=False)
    reducer_code_ref = Column(String(512), nullable=False)
    output_code_ref = Column(String(512), nullable=True)
    num_mappers = Column(Integer, default=3)
    num_reducers = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tasks = relationship("Task", back_populates="job")

class Task(Base):
    __tablename__ = 'tasks'

    task_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Ξένο κλειδί (Foreign Key) που δείχνει στο Job
    job_id = Column(UUID(as_uuid=True), ForeignKey('jobs.job_id'))
    task_type = Column(Enum(TaskType), nullable=False)
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING)
    worker_pod_id = Column(String(255))
    input_partition_ref = Column(String(512), nullable=False)
    output_partition_ref = Column(String(512))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    job = relationship("Job", back_populates="tasks")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_minio():
    
    yield minio_client