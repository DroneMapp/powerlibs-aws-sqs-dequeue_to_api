from powerlibs.aws.sqs.dequeue_to_api.transformations import accumulate


def test_accumulator(dequeuer, simple_message_payload, accumulators, accumulators_responses):
    def url_get(dequeuer, url):
        return accumulators_responses[url]

    results = accumulate(dequeuer, simple_message_payload, accumulators, url_get)
    first_result = results[0]

    assert 'payload' in first_result
    assert first_result['payload'] == simple_message_payload

    assert 'alfa' in first_result
    assert 'beta' in first_result
    assert 'gama' in first_result
    assert first_result['gama']['id'] == 'FINAL_01'
