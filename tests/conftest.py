import json
from unittest import mock

import pytest


from powerlibs.aws.sqs.dequeue_to_api import DequeueToAPI


class Message:
    def __init__(self, data, attributes):
        message = json.dumps(data)
        self.body = json.dumps({
            'Message': message,
            "Timestamp": "2017-04-27T20:38:51.525Z",
        })

        self.delete = mock.Mock()
        self.message_attributes = attributes


@pytest.fixture
def update_message():
    return Message(
        {
            'id': 1,
            'parent_id': 2,
            'company_name': 'mycompany',
        },
        {'topic': 'mycompany__child_created'}
    )


@pytest.fixture
def create_message():
    return Message(
        {
            'id': 1,
            'parent_id': 2,
            'company_name': 'mycompany',
        },
        {'topic': 'mycompany__child_updated'}
    )


@pytest.fixture
def config():
    return {
        'config': {
            'base_url': 'https://{data[company_name]}.example.com/',
        },
        'actions': {
            'update_parent': {
                'topic': '{data[company_name]}__child_created',
                'endpoint': 'parents/{data[parent_id]}',
                'method': 'PATCH',
                'payload': {
                    'status': 'new status',
                }
            },
            'create_status': {
                'topic': '{data[company_name]}__child_updated',
                'endpoint': 'stati/',
                'method': 'POST',
                'payload': {
                    'status': 'created',
                    'parent': '{data[id]}',
                }
            }
        }
    }


@pytest.fixture
def dequeuer(config):
    mocked_requests_method = mock.Mock(
        return_value=mock.Mock(
            status_code=200,
            raise_for_status=mock.Mock(),
        )
    )

    d = DequeueToAPI(
        config, 'TEST QUEUE',
        process_pool_size=0,  # Do not use multiprocessing.
        thread_pool_size=0,  # Do not use threads.
        aws_access_key_id='AWS_ID',
        aws_secret_access_key='AWS_SECRET',
        aws_region='AWS_REGION'
    )

    for method_name in d.request_methods:
        d.request_methods[method_name] = mocked_requests_method

    return d
