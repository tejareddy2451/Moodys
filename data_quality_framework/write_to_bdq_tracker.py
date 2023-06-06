import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import Any
from data_quality_framework.constants import bdqConstants

@dataclass
class DynamoRecord:
    """ Represents the schema of our records stored in DynamoDB """
    execution_id: str
    rule_id: str
    execution_start_time: str = ''
    execution_end_time: str = ''
    execution_status: str = ''
    error_message: str = ''
    _update_expression = 'SET '
    _expression_attribute_values = {}

    def __post_init__(self):
        self._update_expression = 'SET '
        self._expression_attribute_values = {}

    def __setattr__(self, name: str, value: Any) -> None:
        if not name.startswith('_'):
            self._update_expression += f'{name}=:{name}, '
            self._expression_attribute_values[f':{name}'] = value
        super().__setattr__(name, value)

    def create_dynamo_update_statement(self) -> dict:
        return {
            "UpdateExpression": self._update_expression[:-2], #chop off last comma
            "ExpressionAttributeValues": self._expression_attribute_values
        }

    def update_to_table(self, table):
        """
        Calls create_dynamo_update_statement and passes that to table.update_item
        Assumes execution_id is the PK. Returns the dynamo.table.update_item() response.
        """
        dynamodb = boto3.resource(bdqConstants.DYNAMO)
        dynamo_table = dynamodb.Table(table)
        update_statement = self.create_dynamo_update_statement()
        print(f'update statement: {update_statement}')
        try:
            return dynamo_table.update_item(
                Key={
                    'execution_id': self.execution_id,
                    'rule_id': self.rule_id
                },
                ConditionExpression="attribute_exists(execution_id) and attribute_exists(rule_id)",
                **update_statement
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f'Error updating {self.execution_id} - {self.rule_id}')
                print(update_statement)
                raise e

    def insert_to_table(self, table):
        dynamodb = boto3.resource(bdqConstants.DYNAMO)
        dynamo_table = dynamodb.Table(table)
        try:
            dynamo_table.put_item(Item={
                'execution_id': self.execution_id,
                'rule_id': self.rule_id,
                'execution_start_time': self.execution_start_time,
                'execution_end_time': self.execution_end_time,
                'execution_status': self.execution_status,
                'error_message': self.error_message
            })
        except Exception as e:
            print(f'Error inserting {self.execution_id} - {self.rule_id}')
            print(self)
            raise e

def insert_dynamo_record(table, rule_id, exec_id):
    try:
        print(f'WriteToBDQTracker - RULE ID:{rule_id} - Insert into tracker table - Started')
        if isinstance(rule_id, list):
            rule_id = ','.join(rule_id)
        bdq_dynamo_record = DynamoRecord(exec_id.replace('.',''), rule_id)
        bdq_dynamo_record.execution_start_time = str(datetime.now())
        ins_response = bdq_dynamo_record.insert_to_table(table)
        print(f'WriteToBDQTracker - RULE ID:{rule_id} - Insert into tracker table - Completed')
        return
    except Exception as exc:
        print(f'WriteToBDQTracker - RULE ID:{rule_id} - Insert into tracker table - Failed with error:{exc}')
        raise exc


def update_dynamo_record(table, rule_id, exec_id, status, err_msg):
    bdq_dynamo_record = DynamoRecord(exec_id, rule_id)
    bdq_dynamo_record.execution_end_time = str(datetime.now())
    bdq_dynamo_record.execution_status = status
    bdq_dynamo_record.error_message = err_msg if err_msg != '' else ''
    update_response = bdq_dynamo_record.update_to_table(table)