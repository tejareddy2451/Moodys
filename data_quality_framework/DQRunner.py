import logging
from data_quality_framework.dq_util import run_data_quality

def run(job_env, app_name, run_all_rules, rule_id, purge_days):
    if not app_name:
        raise SystemExit('Application Name is needed')

    print('DQRunner - Started')
    print('DQRunner - Read all the job parameters.')
    print(f'DQRunner - App Name : {app_name}')
    print(f'DQRunner - Job Environment : {job_env}')
    print(f'DQRunner - Run All Rules flag : {run_all_rules}')
    print(f'DQRunner - Rule Id : {rule_id}')
    print(f'DQRunner - Purge Days : {purge_days}')

    if not job_env:
        logging.error('DQRunner - Job Environment is needed')
        print('DQRunner - Job Environment is needed')
        raise SystemExit('Job Environment is needed')

    # 2. When Run all parameter is false, pipeline_id is needed
    if run_all_rules != "true" and not rule_id:
        logging.error('DQRunner - Rule id is needed when Run All Rules is false')
        print('DQRunner - Rule id is needed when Run All Rules is false')
        raise SystemExit('DQRunner - Rule id is needed when Run All Rule is false')

    run_data_quality(job_env, app_name, run_all_rules, rule_id, purge_days)