import functools
import json
import os
from decimal import Decimal

from boto3 import resource
from boto3.dynamodb.conditions import Key, Attr


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, Decimal):
            return float(o)

        return super(DecimalEncoder, self).default(o)


class DynamoDBService:
    def __init__(self, table_name, dynamodb=None, table=None):
        region = os.getenv('AWS_REGION', 'us-east-1')
        self.table_name = table_name
        self.dynamodb = dynamodb or resource('dynamodb', region_name=region)
        self.table = table or self.dynamodb.Table(self.table_name)

    def remove_empty_string(self, obj):
        if isinstance(obj, dict):
            return {k: self.remove_empty_string(v) for k, v in obj.items() if k}
        elif isinstance(obj, list):
            return [self.remove_empty_string(v) for v in obj]
        elif isinstance(obj, str) and obj == "":
            return None
        else:
            return obj

    @staticmethod
    def to_update(attributes_update: dict):
        return {k: {'Value': v} for k, v in attributes_update.items()}

    @staticmethod
    def get_key_condition(condition='eq', key_name=None, values=None):
        if not key_name or not values:
            raise Exception(f"Missing key = {key_name} or values = {values}")
        if condition == 'eq':
            return Key(key_name).eq(*values)
        elif condition == 'between':
            return Key(key_name).between(*values)
        elif condition == 'gte':
            return Key(key_name).gte(*values)
        elif condition == 'gt':
            return Key(key_name).gt(*values)
        elif condition == 'lt':
            return Key(key_name).lt(*values)
        elif condition == 'lte':
            return Key(key_name).lte(*values)

    @staticmethod
    def get_attr_condition(condition='eq', key_name=None, values=None):
        if not key_name or not values:
            raise Exception(f"Missing key = {key_name} or values = {values}")
        if condition == 'eq':
            return Attr(key_name).eq(*values)
        elif condition == "contains":
            return Attr(key_name).contains(*values)
        elif condition == 'between':
            return Attr(key_name).between(*values)
        elif condition == 'gte':
            return Attr(key_name).gte(*values)
        elif condition == 'gt':
            return Attr(key_name).gt(*values)
        elif condition == 'lt':
            return Attr(key_name).lt(*values)
        elif condition == 'lte':
            return Attr(key_name).lte(*values)

    @staticmethod
    def get_sort_key_condition(s_key_name, s_key_value_lower, s_key_value_max):
        if s_key_name and s_key_value_max is not None and s_key_value_lower is not None:
            return Key(s_key_name).between(s_key_value_lower, s_key_value_max)
        elif s_key_name and s_key_value_lower is not None and s_key_value_max is None:
            return Key(s_key_name).gte(s_key_value_lower)
        elif s_key_name and s_key_value_max is not None and s_key_value_lower is None:
            return Key(s_key_name).lt(s_key_value_max)
        else:
            return None

    @staticmethod
    def get_partition_key_condition(key_name, key_value):
        return Key(key_name).eq(key_value) if (key_name and key_value is not None) else None

    def get_partition_sort_key_condition(self, p_key_name, p_key_value, s_key_name, s_key_value_lower, s_key_value_max):
        primary_cond = self.get_partition_key_condition(p_key_name, p_key_value)
        sort_cond = self.get_sort_key_condition(s_key_name, s_key_value_lower, s_key_value_max)
        if primary_cond and sort_cond:
            return primary_cond.__and__(sort_cond)
        elif primary_cond:
            return primary_cond
        elif sort_cond:
            return sort_cond
        else:
            return None

    def convert_to_dynamodb(self, data):
        return json.loads(json.dumps(self.remove_empty_string(data)), parse_float=Decimal)

    @staticmethod
    def convert_from_dynamodb(data):
        return json.loads(json.dumps(data, cls=DecimalEncoder))

    def put_items(self, items, batch_writer_params=None):
        batch_writer_params = batch_writer_params or {}
        with self.table.batch_writer(**batch_writer_params) as batch:
            for item in items:
                batch.put_item(Item=self.convert_to_dynamodb(item))
        return len(items)

    def put_item(self, item):
        self.table.put_item(Item=self.convert_to_dynamodb(item))

    def update_item(self, key_map: dict, attributes_updates: dict):
        return self.table.update_item(Key=key_map, AttributeUpdates=self.convert_to_dynamodb(attributes_updates))

    def delete_item(self, key_name, key_value, range_name=None, range_value=None):
        key = {key_name: key_value}
        if range_name and range_value:
            key[range_name] = range_value
        return self.table.delete_item(Key=key)

    def delete_items(self, key_name: str, keys_values: list):
        with self.table.batch_writer() as batch:
            for key_value in list(set(keys_values)):
                batch.delete_item(Key={key_name: key_value})

    def get_batch_items(self, key_name, key_values, sort_key_name=None, sort_key_value=None):
        key_values_no_duplicates = list(set(key_values))
        params = {
            "RequestItems": {
                self.table_name: {
                    "Keys": [{key_name: value, sort_key_name: sort_key_value} if sort_key_name else {key_name: value}
                             for value in key_values_no_duplicates]}
            }
        }
        response = self.dynamodb.batch_get_item(**params)
        responses = response.get("Responses", {})
        items = responses.get(self.table_name, [])
        return self.convert_from_dynamodb(items)

    def get_scan_items(self, p_key_name=None, p_key_value=None, s_key_name=None, s_key_value_lower=None,
                       s_key_value_max=None, page_size=1000, params=None):
        return self.__get_items(self.__scan, p_key_name, p_key_value, s_key_name, s_key_value_lower, s_key_value_max,
                                page_size, params, 'FilterExpression')

    def get_query_items(self, p_key_name=None, p_key_value=None, s_key_name=None, s_key_value_lower=None,
                        s_key_value_max=None, page_size=1000, params=None):
        return self.__get_items(self.__query, p_key_name, p_key_value, s_key_name, s_key_value_lower, s_key_value_max,
                                page_size, params, 'KeyConditionExpression')

    def get_page_scan_items(self, params=None, page_size=1000, p_key_name=None, p_key_value=None, s_key_name=None,
                            s_key_value_lower=None, s_key_value_max=None):
        return self.__get_page_items(self.__scan, params=params, page_size=page_size, p_key_name=p_key_name,
                                     p_key_value=p_key_value, s_key_name=s_key_name,
                                     s_key_value_lower=s_key_value_lower, s_key_value_max=s_key_value_max)

    def get_page_query_items(self, params=None, page_size=1000, p_key_name=None, p_key_value=None, s_key_name=None,
                             s_key_value_lower=None, s_key_value_max=None):
        return self.__get_page_items(self.__query, params=params, page_size=page_size, p_key_name=p_key_name,
                                     p_key_value=p_key_value, s_key_name=s_key_name,
                                     s_key_value_lower=s_key_value_lower, s_key_value_max=s_key_value_max)

    def __get_page_items(self, search_function, p_key_name=None, p_key_value=None, s_key_name=None,
                         s_key_value_lower=None, s_key_value_max=None, params=None, page_size=None):
        condition = self.get_partition_sort_key_condition(p_key_name, p_key_value, s_key_name, s_key_value_lower,
                                                          s_key_value_max)
        params = self.get_params(initial_params=params, condition=condition, page_size=page_size)
        return self.convert_from_dynamodb(search_function(params).get("Items", []))

    def scan_items(self, params=None, total_items=None):
        return self.__get_items_db(search_function=self.__scan, params=params, total_items=total_items)

    def query_items(self, params=None, total_items=None):
        return self.__get_items_db(search_function=self.__query, params=params, total_items=total_items)

    def __get_items_db(self, search_function, params=None, total_items=None):

        results = []
        response = search_function(params)
        results.extend(response['Items'])

        if total_items and len(results) >= total_items:
            return results[0:total_items]

        while 'LastEvaluatedKey' in response:
            params = self.get_params(initial_params=params, last_evaluated_key=response['LastEvaluatedKey'])
            response = search_function(params)
            results.extend(response['Items'] or [])
            if total_items and len(results) >= total_items:
                return results[0:total_items]

        return self.convert_from_dynamodb(results)

    def __get_items(self, search_function, p_key_name, p_key_value, s_key_name, s_key_value_lower, s_key_value_max,
                    page_size, params, condition_key):
        condition = self.get_partition_sort_key_condition(p_key_name, p_key_value, s_key_name, s_key_value_lower,
                                                          s_key_value_max)

        results = []
        params = self.get_params(condition_key, condition, page_size=page_size, initial_params=params)
        response = search_function(params)
        results.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            params = self.get_params(condition, last_evaluated_key=response['LastEvaluatedKey'], page_size=page_size,
                                     initial_params=params)
            response = search_function(params)
            results.extend(response['Items'] or [])

        return self.convert_from_dynamodb(results)

    def build_key_filters(self, key_filters):
        filters_dynamo = [self.get_key_condition(condition=to_filter.get("condition", 'eq'),
                                                 key_name=to_filter.get("key"), values=to_filter.get("values"))
                          for to_filter in key_filters]
        if len(filters_dynamo) == 1:
            return filters_dynamo[0]
        if len(filters_dynamo) > 1:
            return functools.reduce(lambda total, item: total & item, filters_dynamo)

    def build_filter_expression(self, filters):
        """

        :param filters: JSON [{'key'L '', 'values': '', 'condition': ''}, ..]
        :return:
        """
        filters_dynamo = [self.get_attr_condition(condition=to_filter.get("condition", 'eq'),
                                                  key_name=to_filter.get("key"), values=to_filter.get("values"))
                          for to_filter in filters]
        if len(filters_dynamo) == 1:
            return filters_dynamo[0]
        if len(filters_dynamo) > 1:
            return functools.reduce(lambda total, item: total & item, filters_dynamo)

    def get_params(self, condition_key='KeyConditionExpression', condition=None,
                   last_evaluated_key=None, index_name=None,
                   page_size=1000, initial_params=None, filters=None, sort=None,
                   key_filters=None):
        """

        :param key_filters:
        :param condition_key:
        :param condition:
        :param last_evaluated_key: Map
        :param index_name: String
        :param page_size: Int
        :param initial_params: Map
        :param filters: Array<{'condition': <STRING>, 'key'L <STRING>, 'values': <ARRAY>}>
        :param sort: Boolean
        :return:
        """
        query_params = initial_params or {}
        if key_filters:
            filters_expression = self.build_key_filters(key_filters)
            query_params.update({'KeyConditionExpression': filters_expression})
        if filters:
            filters_expression = self.build_filter_expression(filters)
            query_params.update({'FilterExpression': filters_expression})
        if sort is not None:
            query_params.update({'ScanIndexForward': sort})
        if page_size:
            query_params.update({'Limit': page_size})
        if last_evaluated_key:
            query_params.update({'ExclusiveStartKey': last_evaluated_key})
        if condition:
            query_params.update({condition_key: condition})
        if index_name:
            query_params.update({'IndexName': index_name})
        return query_params

    def __query(self, params):
        """
        For internal use - please use get_query_items
        :param params: (Limit, ExclusiveStartKey, KeyConditionExpression
        :return: {'Items': [items]}
        """
        return self.table.query(**params)

    def __scan(self, params):
        """
        For internal use - please use get_scan_items
        :param params: (Limit, ExclusiveStartKey, KeyConditionExpression
        :return: {'Items': [items]}
        """
        return self.table.scan(**params)
