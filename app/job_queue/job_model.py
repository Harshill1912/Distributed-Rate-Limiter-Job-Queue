from  pydantic import BaseModel

class Job(BaseModel):
      job_id:str
      task_type:str
      payload:dict
      status:str="pending"
      retry_count:int=0