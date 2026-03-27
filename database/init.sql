CREATE TYPE job_status AS ENUM ('SUBMITTED', 'RUNNING', 'COMPLETED', 'FAILED');
CREATE TYPE task_status AS ENUM ('PENDING', 'RUNNING', 'RETRYING', 'COMPLETED', 'FAILED');
CREATE TYPE task_type AS ENUM ('MAP', 'REDUCE');

CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    status job_status DEFAULT 'SUBMITTED',
    input_code_ref VARCHAR(512) NOT NULL,
    mapper_code_ref VARCHAR(512) NOT NULL,
    reducer_code_ref VARCHAR(512) NOT NULL,
    output_code_ref VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id UUID PRIMARY KEY,
    job_id UUID REFERENCES jobs(job_id),
    task_type task_type NOT NULL,
    status task_status DEFAULT 'PENDING',
    worker_pod_id VARCHAR(255),
    input_partition_ref VARCHAR(512) NOT NULL,
    output_partition_ref VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


