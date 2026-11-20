import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://trace:tracepass@db:5432/operatortrace')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
SNAPSHOT_LIMIT_BYTES=int(os.getenv('SNAPSHOT_LIMIT_BYTES','1048576'))
