from collections import defaultdict
from functools import partial
import glob
import importlib
import json
import os.path
import sys

import requests

from powerlibs.aws.sqs.dequeuer import SQSDequeuer
from .transformations import accumulate, apply_data_map


class DequeueToAPI(SQSDequeuer):

    request_methods = {}

    def __init__(self, config_data, queue_name, *args, **kwargs):
        super().__init__(queue_name, None, *args, **kwargs)
        self.load_config(config_data)

        self.load_request_methods()

        self.custom_handlers = {}
        self.load_custom_handlers(self.config.get('custom_handlers', {}))

    @property
    def requests_headers(self):
        return {}

    def load_request_methods(self, requests_module=None):
        requests_module = requests_module or requests

        def do_request(request_method, *args, **kwargs):
            headers = kwargs.get('headers', {})
            headers.update(self.requests_headers)
            kwargs['headers'] = headers
            response = request_method(*args, **kwargs)
            response.raise_for_status()
            return response

        for method_name in ('get', 'post', 'patch', 'delete', 'put'):
            method = getattr(requests_module, method_name)
            self.request_methods[method_name] = partial(do_request, method)

    def load_config(self, config_data):
        self.config = config_data['config']
        self.actions = config_data['actions']
        self.topics = defaultdict(list)

        for action_name, action_data in self.actions.items():
            topic_name = action_data['topic']

            self.topics[topic_name].append((action_name, action_data))

    def hydrate_payload(self, payload_template, payload):
        hydrated_payload = {}
        for key, value in payload_template.items():
            hydrated_payload[key] = value.format(payload=payload)

        return hydrated_payload

    def hydrate_action(self, action, payload):
        endpoint = action.get('endpoint', None)
        if endpoint:
            self.hydrate_action_with_endpoint(action, payload, endpoint)

        custom_handlers = action.get('custom_handlers', None)
        if custom_handlers:
            self.hydrate_action_with_custom_handlers(action, payload, custom_handlers)

    def hydrate_action_with_endpoint(self, action, payload, endpoint):
        url_str = os.path.join(self.config['base_url'], endpoint)
        url = url_str.format(config=self.config, payload=payload)
        request_method = self.request_methods[action['method'].lower()]

        accumulators = action.get('accumulators', [])
        accumulation_entries = accumulate(payload, accumulators)

        data_map = action.get('data_map', {})
        mapped_entries = [apply_data_map(entry, data_map) for entry in accumulation_entries]

        def run(the_request_method, the_mapped_entries):
            for entry in the_mapped_entries:
                the_request_method(url, payload=entry)

        partial_run = partial(run, request_method, mapped_entries)
        action['run'] = partial_run

    def load_custom_handlers(self, config):
        for path in config.get('paths', []):
            self.load_custom_handlers_from_path(path)

    def load_custom_handlers_from_path(self, path):
        assert os.path.isdir(path)

        sys.path.append(path)

        globbing_str = '{}/*.py'.format(path)
        for filepath in glob.glob(globbing_str):
            filename = os.path.basename(filepath)
            name, extension = os.path.splitext(filename)
            module = importlib.import_module(name)
            the_plugin_class = getattr(module, 'Plugin')
            self.custom_handlers[name] = the_plugin_class(self)

    def get_custom_handler(self, name):
        if name not in self.custom_handlers:
            plugin_name, function_name = name.split('.')
            plugin = self.custom_handlers[plugin_name]
            function = getattr(plugin, function_name)
            self.custom_handlers[name] = function

        return self.custom_handlers[name]

    def hydrate_action_with_custom_handlers(self, action, payload, custom_handlers_names):
        def run_handlers(action, payload, the_handlers):
            for handler in the_handlers:
                handler(action, payload)

        handlers = [self.get_custom_handler(name) for name in custom_handlers_names]
        partial_run = partial(run_handlers, action, payload, handlers)

        action['run'] = partial_run

    def get_actions_for_topic(self, topic, payload):
        for topic_name, actions in self.topics.items():
            expanded_topic_name = topic_name.format(config=self.config, payload=payload)
            if expanded_topic_name == topic:
                for action_name, action_data in actions:
                    self.hydrate_action(action_data, payload)
                    yield (action_name, action_data)

    def do_handle_message(self, message, topic, payload):
        success_count = 0
        treated_messages = 0
        for action_name, action_data in self.get_actions_for_topic(topic, payload):
            try:
                action_data['run']()
            except Exception as ex:
                self.logger.error('Exception {ex_type} on topic "{topic}", action "{action_name}": {ex}'.format(
                    ex_type=type(ex),
                    topic=topic,
                    action_name=action_name,
                    ex=ex
                ))

                if success_count == 0:
                    return
            else:
                success_count += 1
        else:
            treated_messages += 1
            message.delete()

        return treated_messages

    def parse_and_handle_message(self, message):
        payload = json.loads(message.body)

        attributes = message.message_attributes
        topic = attributes['topic']['StringValue']

        return self.do_handle_message(message, topic, payload)

    def handle_message(self, message):
        if self.thread_pool_size:
            self.execute_new_thread(self.parse_and_handle_message, [message])
        else:
            self.parse_and_handle_message(message)
