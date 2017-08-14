from collections import defaultdict
from functools import partial
import json
import os.path

import requests

from powerlibs.aws.sqs.dequeuer import SQSDequeuer


class DequeueToAPI(SQSDequeuer):
    def __init__(self, config_data, queue_name, *args, **kwargs):
        super().__init__(queue_name, None, *args, **kwargs)
        self.load_config(config_data)

        self.load_request_methods()

    def load_request_methods(self):
        self.request_methods = {}

        for method_name in ('get', 'post', 'patch', 'delete'):
            self.request_methods[method_name] = getattr(requests, method_name)

    def load_config(self, config_data):
        self.config = config_data['config']
        self.actions = config_data['actions']
        self.topics = defaultdict(list)

        for action_name, action_data in self.actions.items():
            topic_name = action_data['topic']

            self.topics[topic_name].append((action_name, action_data))

    def hydrate_payload(self, payload_template, data):
        payload = {}
        for key, value in payload_template.items():
            payload[key] = value.format(data=data)

        return payload

    def hydrate_action(self, action, data):
        url_str = os.path.join(self.config['base_url'], action['endpoint'])
        url = url_str.format(config=self.config, data=data)
        action['url'] = url

        method = action['method']

        payload = self.hydrate_payload(action['payload'], data)

        requests_method = self.request_methods[method.lower()]
        action['do_request'] = partial(requests_method, url, data=payload)

    def get_actions_for_topic(self, topic, data):
        for topic_name, actions in self.topics.items():
            expanded_topic_name = topic_name.format(config=self.config, data=data)
            if expanded_topic_name == topic:
                for action_name, action_data in actions:
                    self.hydrate_action(action_data, data)
                    yield (action_name, action_data)

    def do_handle_message(self, message, topic, payload):
        success_count = 0
        treated_messages = 0
        for action_name, action_data in self.get_actions_for_topic(topic, payload):
            try:
                response = action_data['do_request']()
                response.raise_for_status()
            except Exception as ex:
                self.logger.error('Exception on topic "{topic}", action "{action_name}": {ex}'.format(
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
        topic = attributes['topic']

        return self.do_handle_message(message, topic, payload)

    def handle_message(self, message):
        if self.thread_pool_size:
            self.execute_new_thread(self.parse_and_handle_message, [message])
        else:
            self.parse_and_handle_message(message)
