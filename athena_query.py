import time
from typing import Union

import boto3


def execute_athena_query(
        query: str,
        output_path: str,
        sleep_interval: int = 5,
        database='aws_meetup'
) -> Union[bool, str]:

    client = boto3.client('athena', region_name='eu-central-1')
    query_resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_path},
    )

    task_id = query_resp['QueryExecutionId']

    while True:
        status_resp = client.get_query_execution(QueryExecutionId=task_id)
        status = status_resp['QueryExecution']['Status']['State'].upper()
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(sleep_interval)

    if status != 'SUCCEEDED':
        raise ValueError(f'Status: {status}')

    return status_resp['QueryExecution']['ResultConfiguration']['OutputLocation']


if __name__ == '__main__':
    print(
        execute_athena_query(
            """
        select
            vendorid as vendor_id,
            sum(extra) as extra_money
        from dl_yellow_taxi_ds
        where
            pickup_date >= '2019-03-01'
            and pickup_date < '2019-03-07'
        group by 1
        order by 2 desc
        """,
            output_path='s3://aws-meetup-almaty/athena-query-results/',
        )
    )
