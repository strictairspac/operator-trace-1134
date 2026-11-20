from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey,UniqueConstraint
from src.config.database import Base
class InputSnapshot(Base):
 __tablename__='input_snapshots'
 id=Column(Integer,primary_key=True)
 dataset_id=Column(Integer,ForeignKey('datasets.id'),nullable=False,index=True)
 digest=Column(String(64),nullable=False)
 size_bytes=Column(Integer,nullable=False)
 payload=Column(JSON,nullable=False)
 created_at=Column(DateTime,server_default=func.now())
 __table_args__=(UniqueConstraint('dataset_id','digest',name='uq_input_snapshots_dataset_digest'),)
