import json
from unittest import mock

import pytest


from powerlibs.aws.sqs.dequeue_to_api import DequeueToAPI


class Message:
    def __init__(self, data, attributes):
        self.body = json.dumps(data)

        self.delete = mock.Mock()
        self.message_attributes = attributes


@pytest.fixture
def simple_message_payload():
    return {
        'id': 'MESSAGE_ID',
        'company_name': 'mycompany',
    }


@pytest.fixture
def update_message():
    return Message(
        {
            'id': 1,
            'parent_id': 2,
            'company_name': 'mycompany',
        },
        {'topic': {'StringValue': 'mycompany__child_created'}}
    )


@pytest.fixture
def create_message():
    return Message(
        {
            'id': 1,
            'parent_id': 2,
            'company_name': 'mycompany',
        },
        {'topic': {'StringValue': 'mycompany__child_updated'}}
    )


@pytest.fixture
def born_message():
    return Message(
        {
            'id': 1,
            'parent_id': 2,
            'company_name': 'mycompany',
        },
        {'topic': {'StringValue': 'mycompany__child_is_born'}}
    )


@pytest.fixture
def config():
    return {
        'config': {
            'base_url': 'https://{payload[company_name]}.example.com/',
        },
        'actions': {
            'update_parent': {
                'topic': '{payload[company_name]}__child_created',
                'endpoint': 'parents/{payload[parent_id]}',
                'method': 'PATCH',
                'payload': {
                    'status': 'new status',
                    'foobar': 'OPTIONAL:{payload[foobar]}'
                }
            },
            'create_status': {
                'topic': '{payload[company_name]}__child_updated',
                'endpoint': 'stati/',
                'method': 'POST',
                'payload': {
                    'status': 'created',
                    'parent': '{payload[id]}',
                }
            },
            'test_regexp_matching': {
                'topic': 'object__\w+',
                'endpoint': 'test/',
                'method': 'POST',
            },
            'test_regexp_matching_with_groups': {
                'topic': 'step__(?P<step_name>[^_]+)__(?P<step_status>[^_]+)',
                'endpoint': 'steps/',
                'method': 'POST',
                'payload': {
                    'status': '{_topic_groups[step_status]}',
                    'name': '{_topic_groups[step_name]}'
                }
            },
            'test_not_optional_missing': {
                'topic': '{payload[company_name]}__child_is_born',
                'endpoint': 'parents/{payload[parent_id]}',
                'method': 'PATCH',
                'payload': {
                    'status': 'new status',
                    'foobar_not_optional': '{payload[foobar]}'
                }
            },
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

    class MyDequeuer(DequeueToAPI):
        @property
        def requests_headers(self):
            return {'Authorization': 'token TEST-TOKEN'}

    d = MyDequeuer(
        config, 'TEST QUEUE',
        process_pool_size=0,  # Do not use multiprocessing.
        thread_pool_size=0,  # Do not use threads.
        aws_access_key_id='AWS_ID',
        aws_secret_access_key='AWS_SECRET',
        aws_region='AWS_REGION'
    )

    mocked_requests_module = mock.Mock(
        get=mocked_requests_method,
        post=mocked_requests_method,
        patch=mocked_requests_method,
        delete=mocked_requests_method,
    )
    d.mocked_requests_module = mocked_requests_module
    d.mocked_requests_method = mocked_requests_method
    d.load_request_methods(mocked_requests_module)

    return d


@pytest.fixture
def accumulators():
    return (
        ('alfa', 'foo/{payload[id]}/'),
        ('beta', 'bar/?parent={alfa[id]}'),
        ('gama', 'baz/{beta[baz_id]}')
    )


@pytest.fixture
def accumulators_responses():
    return {
        'https://mycompany.example.com/foo/MESSAGE_ID/': (
            {'id': 'ALFA_ID_01'},
            {'id': 'ALFA_ID_02'},
        ),
        'https://mycompany.example.com/bar/?parent=ALFA_ID_01': (
            {'id': 'BETA_ID_01', 'baz_id': 'BAZ_ID_01'},
            {'id': 'BETA_ID_02', 'baz_id': 'BAZ_ID_02'},
        ),
        'https://mycompany.example.com/bar/?parent=ALFA_ID_02': (
            {'id': 'BETA_ID_01', 'baz_id': 'BAZ_ID_11'},
            {'id': 'BETA_ID_02', 'baz_id': 'BAZ_ID_12'},
        ),
        'https://mycompany.example.com/baz/BAZ_ID_01': (
            {'id': 'FINAL_01'},
            {'id': 'FINAL_02'},
        ),
        'https://mycompany.example.com/baz/BAZ_ID_02': (
            {'id': 'FINAL_03'},
        ),
        'https://mycompany.example.com/baz/BAZ_ID_11': (
            {'id': 'FINAL_04'},
            {'id': 'FINAL_05'},
            {'id': 'FINAL_06'},
        ),
        'https://mycompany.example.com/baz/BAZ_ID_12': (
            {'id': 'FINAL_07'},
        ),
    }


@pytest.fixture
def data_map():
    return {
        'alfa': 'A',
        'beta': 'B',
    }
