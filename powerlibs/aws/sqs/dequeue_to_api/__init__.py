from collections import defaultdict
from functools import partial
import glob
import importlib
import json
import os.path
import re
import sys
import traceback

import requests

from powerlibs.aws.sqs.dequeuer import SQSDequeuer
from .transformations import accumulate, apply_data_map


class DequeueToAPI(SQSDequeuer):
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
        self.request_methods = {}

        def do_request(request_method, *args, **kwargs):
            headers = kwargs.get('headers', {})
            headers.update(self.requests_headers)
            kwargs['headers'] = headers
            response = request_method(*args, **kwargs)

            method_name = request_method.__name__
            if method_name == 'delete' and response.status_code == 404:
                return response

            try:
                response.raise_for_status()
            except Exception as ex:
                print('do_request:', method_name, ':', args, kwargs)
                print(response.content)
                raise ex
            return response

        for method_name in ('get', 'post', 'patch', 'delete', 'put'):
            method = getattr(requests_module, method_name)
            requester = partial(do_request, method)
            self.request_methods[method_name] = requester
            setattr(self, method_name, requester)

    def load_config(self, config_data):
        self.config = config_data['config']
        self.actions = config_data['actions']
        self.topics = defaultdict(list)

        for action_name, action_data in self.actions.items():
            topic_name = action_data['topic']

            self.topics[topic_name].append((action_name, action_data))

    def apply_payload_template(self, payload_template, topic, topic_groups, action, payload):
        hydrated_payload = {}
        for key, value in payload_template.items():

            optional = False
            if value.startswith('OPTIONAL:'):
                optional = True
                value = value.replace('OPTIONAL:', '')

            try:
                hydrated_payload[key] = value.format(
                    _topic=topic,
                    _topic_groups=topic_groups,
                    _action=action,
                    **payload
                )
            except KeyError as ex:
                if optional:
                    continue
                raise ex

            if hydrated_payload[key].startswith('INT:'):
                hydrated_payload[key] = int(hydrated_payload[key].replace('INT:', ''))
            elif hydrated_payload[key].startswith('DICT:'):
                hydrated_payload[key] = dict(hydrated_payload[key].replace('DICT:', ''))
            elif hydrated_payload[key].startswith('EVAL:'):
                hydrated_payload[key] = eval(hydrated_payload[key].replace('EVAL:', ''))

        return hydrated_payload

    def hydrate_action(self, topic, topic_groups, action, payload):
        action['message_topic'] = topic  # TODO: deprecate this!
        endpoint = action.get('endpoint', None)
        if endpoint:
            self.hydrate_action_with_endpoint(topic, topic_groups, action, payload, endpoint)

        custom_handlers = action.get('custom_handlers', None)
        if custom_handlers:
            self.hydrate_action_with_custom_handlers(action, topic, payload, custom_handlers)

    def endpoint_run(self, request_method_name, url, the_entries):
        request_method = self.request_methods[request_method_name]
        for entry in the_entries:
            if len(entry) == 1 and 'payload' in entry:
                request_method(url, json=entry['payload'])
            else:
                request_method(url, json=entry)

    def hydrate_action_with_endpoint(self, topic, topic_groups, action, payload, endpoint):
        url_str = os.path.join(self.config['base_url'], endpoint)
        url = url_str.format(config=self.config, payload=payload, topic=topic)

        accumulators = action.get('accumulators', [])
        accumulation_entries = accumulate(self, payload, accumulators)

        payload_template = action.get('payload', None)
        if payload_template:
            hydrated_entries = [
                self.apply_payload_template(payload_template, topic, topic_groups, action, entry)
                for entry in accumulation_entries
                if entry
            ]
        else:
            hydrated_entries = accumulation_entries

        data_map = action.get('data_map', {})
        mapped_entries = [apply_data_map(entry, data_map) for entry in hydrated_entries]

        partial_run = partial(self.endpoint_run, action.get('method').lower(), url, mapped_entries)
        action['run'] = partial_run

    def load_custom_handlers(self, config):
        for path in config.get('paths', []):
            self.load_custom_handlers_from_path(path)

    def load_custom_handlers_from_path(self, path):
        assert os.path.isdir(path)

        sys.path.append(path)

        globbing_str = '{}/*/plugin.py'.format(path)
        for filepath in glob.glob(globbing_str):
            self.load_custom_handler(filepath)

    def load_custom_handler(self, filepath):
        dirname = os.path.basename(os.path.dirname(filepath))
        module_name = '{}.plugin'.format(dirname)
        module = importlib.import_module(module_name)
        try:
            the_plugin_class = getattr(module, 'Plugin')
        except AttributeError:
            return

        self.custom_handlers[dirname] = the_plugin_class(self)

    def get_custom_handler(self, name):
        if name not in self.custom_handlers:
            plugin_name, function_name = name.split('.')
            plugin = self.custom_handlers[plugin_name]
            function = getattr(plugin, function_name)
            self.custom_handlers[name] = function

        return self.custom_handlers[name]

    def run_custom_handlers(self, action, topic, payload, the_handlers):
        for handler in the_handlers:
            handler(action, topic, payload)

    def hydrate_action_with_custom_handlers(self, action, topic, payload, custom_handlers_names):
        handlers = [self.get_custom_handler(name) for name in custom_handlers_names]
        partial_run = partial(self.run_custom_handlers, action, topic, payload, handlers)

        action['run'] = partial_run

    def get_actions_for_topic(self, topic, payload):
        for topic_name, actions in self.topics.items():
            expanded_topic_name = topic_name.format(config=self.config, payload=payload)

            match = re.match(expanded_topic_name, topic)
            if match:
                topic_groups = match.groupdict()
                for action_name, action_data in actions:
                    self.hydrate_action(topic, topic_groups, action_data, payload)
                    yield (action_name, action_data)

    def do_handle_message(self, message, topic, payload):
        treated_messages = 0
        for action_name, action_data in self.get_actions_for_topic(topic, payload):
            try:
                action_data['run']()
            except Exception as ex:
                cls = ex.__class__.__name__
                type_, value_, traceback_ = sys.exc_info()
                formatted_traceback = traceback.format_tb(traceback_)
                pretty_traceback = ''.join(formatted_traceback)

                self.logger.error(
                    f'Exception {cls} on topic "{topic}", action "{action_name}": '
                    f'{value_}. Traceback: {pretty_traceback}'
                )

                response = getattr(ex, 'response', None)
                if response is not None:
                    content = getattr(response, 'content', None)
                    if content is None:
                        content = getattr(response, 'text', None)
                    if content is None:
                        content = response
                    self.logger.error(' Response: {}'.format(content))
                break
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
        self.execute_new_thread(self.parse_and_handle_message, [message])
