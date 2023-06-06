import boto3
from boto3.dynamodb.conditions import Key, Attr
from data_quality_framework.constants import bdqConstants
from data_quality_framework.bdq_common_util import get_region
from data_quality_framework.write_to_bdq_tracker import update_dynamo_record

dynamodb = boto3.resource(bdqConstants.DYNAMO, region_name=get_region())

def query_bdq_inv(filter_col, filter_val, table_nm):
    table = dynamodb.Table(table_nm)
    return_response_items = []
    for val in filter_val:
        print(f'QueryInventoryTable - {filter_col}:{val} - Get user expectations - Started')
        try:
            # looping here to preserve the order
            response = table.scan(
                FilterExpression=Attr(filter_col).eq(val)  #Attr(filter_col).is_in(filter_val)
            )
            response_items = response[bdqConstants.ITEMS]
            while bdqConstants.LAST_EVAL_KEY in response:
                response = table.scan(FilterExpression=Attr(filter_col).eq(val), #.is_in(filter_val),
                                      ExclusiveStartKey=response[bdqConstants.LAST_EVAL_KEY])
                response_items.extend(response[bdqConstants.ITEMS])
            print(f'QueryInventoryTable - {filter_col}:{val} - Get user expectations - Completed')
            return_response_items.extend(response_items)
        except Exception as exc:
            print(f'QueryInventoryTable - {filter_col}:{val} - Get user expectations - Failed with error - {exc}')
            raise exc
    return return_response_items



def get_user_expectations(user_exp, user_col_nms, user_values, table_nm, user_rule_id, tracker_table, execution_id):
    print(f'ReadDynamoInput - RULE ID:{user_rule_id} - Get user expectations - Started')
    rules_map = query_bdq_inv(bdqConstants.EXP_STMT, [exp.lower().strip() for exp in user_exp], table_nm)
    rules_map_exp_method_list, arg1_list, method_inp_args_list, arg2_list = [], [], [], []
    for rule in rules_map:
        ind = rules_map.index(rule)
        method_args = rule[bdqConstants.EXP_METHOD_ARGS].split(',')
        arg1 = user_col_nms[ind].lower()
        arg2 = ""
        method_inp_args = ""
        if len(method_args) > 1:
            if user_values[ind].strip() == "":
                print(f"ReadDynamoInput - RULE ID:{user_rule_id[ind]} - Get user expectations - Failed with error: User's expectation requires input values")
                update_dynamo_record(tracker_table, user_rule_id[ind], execution_id, bdqConstants.FAILED,
                                     f"User statement failed to convert to expectation")
                raise SystemExit("Failed with error: User's expectation requires input values")
            arg2 = method_args[1].strip()
            if arg2 == bdqConstants.USER_VALS_LIST:
                inp_list_vals = [eval(inp) for inp in user_values[ind].split(',')]
                if len(inp_list_vals) != 2:
                    print(
                        f"ReadDynamoInput - RULE ID:{user_rule_id[ind]} - Get user expectations - Failed with error: User's expectation requires 2 input values but {len(inp_list_vals)} passed for list")
                    update_dynamo_record(tracker_table, user_rule_id[ind], execution_id, bdqConstants.FAILED,
                                         f"User statement failed to convert to expectation")
                    raise SystemExit(f"Failed with error: User's expectation requires 2 input values but {len(inp_list_vals)} passed for list")
                method_inp_args = inp_list_vals
            elif arg2 == bdqConstants.USER_COL_VAL:
                if "," in user_values[ind]:
                    print(
                        f"ReadDynamoInput - RULE ID:{user_rule_id[ind]} - Get user expectations - Failed with error: User's expectation requires 1 input value but more than 1 passed")
                    update_dynamo_record(tracker_table, user_rule_id[ind], execution_id, bdqConstants.FAILED,
                                         f"User statement failed to convert to expectation")
                    raise SystemExit("Failed with error: User's expectation requires 1 input value but more than 1 passed")
                method_inp_args = user_values[ind]
            elif arg2 == bdqConstants.USER_COL_VALS:
                if "," not in user_values[ind]:
                    print(
                        f"ReadDynamoInput - RULE ID:{user_rule_id[ind]} - Get user expectations - Failed with error: User's expectation requires more than 1 but only 1 passed")
                    update_dynamo_record(tracker_table, user_rule_id[ind], execution_id, bdqConstants.FAILED,
                                         f"User statement failed to convert to expectation")
                    raise SystemExit("Failed with error: User's expectation requires more than 1 but only 1 passed")
                method_inp_args = user_values[ind]
            print(f'ReadDynamoInput - RULE ID:{user_rule_id[ind]} - method input args - {method_inp_args}')
            print(f'ReadDynamoInput - RULE ID:{user_rule_id[ind]} - Get user expectations - Ended')
        rules_map_exp_method_list.append(rule['expectation_method'])
        arg1_list.append(arg1),
        method_inp_args_list.append(method_inp_args)
        arg2_list.append(arg2)
    return rules_map_exp_method_list, arg1_list, method_inp_args_list, arg2_list
