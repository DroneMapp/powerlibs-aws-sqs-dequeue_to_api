import os


def url_get(dequeuer, url):
    response = dequeuer.get(url)
    response_data = response.json()

    if 'results' in response_data:
        return response_data['results']
    else:
        return [response_data]


def accumulate(dequeuer, payload, accumulators, url_getter=None):
    last_level = [{'payload': payload}]
    url_getter = url_getter or url_get

    for step_name, base_url in accumulators:
        # "ticket", "v1/tickets/{data[ticket]}"
        new_level = []
        url = base_url

        for entry in last_level:
            # last_level = [{"data": {"id": 1, "ticket": "2"}}, ...]

            # 1- Find the right URL:
            for entry_name, entry_values in entry.items():
                # "data" , {"id": 1, "ticket": "2"}
                url = url.format(**{entry_name: entry_values})

            # 2- Save the URL:
            base_url = dequeuer.config['base_url']
            kwargs = {**entry, 'config': dequeuer.config}
            real_url = os.path.join(base_url, url).format(**kwargs)
            for result in url_getter(dequeuer, real_url):
                new_entry = {step_name: result}  # "ticket": {...}
                new_entry.update(entry)  # + "data": {...}
                new_level.append(new_entry)  # [{"ticket": {"id": 1}, "data": {...}}, {"ticket": {"id": 2}, "data": {...}}]

        last_level = new_level

    return last_level


def apply_data_map(data, data_map):
    mapped = {}
    for key, value in data.items():
        if isinstance(value, (str, bytes)) and value.startswith('MAP:'):
            _, *map_key_parts = value.split(':')  # NOQA
            map_key = ':'.join(map_key_parts)

            if map_key and map_key in data_map:
                mapped[key] = data_map[map_key]

        else:
            mapped[key] = value

    return mapped
