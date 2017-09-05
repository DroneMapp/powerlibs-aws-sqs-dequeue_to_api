from powerlibs.aws.sqs.dequeue_to_api.transformations import apply_data_map


def test_data_map(data_map):
    result = apply_data_map({
        'key1': 'value1',
        'key2': 'MAP:alfa',
        'key3': 'MAP:beta',
        'key4': 'aMAP:alfa',
        'key5': 'MAP:',
        'key6': 'MAP:zeta',
    }, data_map)

    assert result['key1'] == 'value1'
    assert result['key2'] == 'A'
    assert result['key3'] == 'B'
    assert result['key4'] == 'aMAP:alfa'

    assert 'key5' not in result
    assert 'key6' not in result
