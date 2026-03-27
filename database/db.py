import enum
import uuid
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Enum, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# 1. Σύνδεση με τη Βάση Δεδομένων (DDS)
SQLALCHEMY_DATABASE_URL = "postgresql://mapreduce_user:mapreduce_password@localhost:5432/mapreduce_db"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. Ορισμός των Enums για τις καταστάσεις (Statuses)
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

# 3. Τα Models μας (Η αντιστοίχιση των πινάκων σε Python Classes)
class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String(255), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.SUBMITTED)
    input_code_ref = Column(String(512), nullable=False)
    mapper_code_ref = Column(String(512), nullable=False)
    reducer_code_ref = Column(String(512), nullable=False)
    output_code_ref = Column(String(512), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Αμφίδρομη σχέση (Relationship) με τον πίνακα tasks
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

    # Αμφίδρομη σχέση (Relationship) με τον πίνακα jobs
    job = relationship("Job", back_populates="tasks")