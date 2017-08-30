def test_get_method(dequeuer):
    dequeuer.request_methods['get']('https://example.com/')

    mocked_get = dequeuer.mocked_requests_method
    assert mocked_get.call_count == 1

    args, kwargs = mocked_get.call_args
    assert args[0] == 'https://example.com/'
    assert 'Authorization' in kwargs['headers']
