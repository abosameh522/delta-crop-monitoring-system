from __future__ import annotations
from prefect import flow, task

@task(retries=0)
def scheduled_noop(years:list[int], in_progress_year:int|None):
    return {'status':'no_op','years':years,'in_progress_year':in_progress_year}

@flow(name='delta-crops-phase6a')
def scheduled_flow(years:list[int], in_progress_year:int|None=None):
    return scheduled_noop(years,in_progress_year)
