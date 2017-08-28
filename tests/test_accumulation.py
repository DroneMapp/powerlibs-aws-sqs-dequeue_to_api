from powerlibs.aws.sqs.dequeue_to_api.transformations import accumulate


def test_accumulator(simple_message_payload, accumulators, accumulators_responses):
    def url_get(url):
        return accumulators_responses[url]

    base_level = {'message': simple_message_payload}
    results = accumulate(base_level, accumulators, url_get)
    first_result = results[0]

    assert 'message' in first_result
    assert first_result['message'] == simple_message_payload

    assert 'alfa' in first_result
    assert 'beta' in first_result
    assert 'gama' in first_result
    assert first_result['gama']['id'] == 'FINAL_01'
