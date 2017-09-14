def test_topic_regexp_matching(dequeuer):
    msg = {'company_name': 'test_company'}
    actions_1 = tuple(dequeuer.get_actions_for_topic('object__created', msg))
    actions_2 = tuple(dequeuer.get_actions_for_topic('object__deleted', msg))
    actions_3 = tuple(dequeuer.get_actions_for_topic('otherthing__created', msg))

    assert actions_1 == actions_2
    assert actions_1 != actions_3


def test_topic_regexp_matching_with_groups(dequeuer):
    msg = {'company_name': 'test_company'}
    actions_1 = tuple(dequeuer.get_actions_for_topic('step__alfa__started', msg))

    payload = actions_1[0][1]['run'].args[2][0]
    assert 'name' in payload
    assert payload['name'] == 'alfa'
    assert 'status' in payload
    assert payload['status'] == 'started', payload

    actions_2 = tuple(dequeuer.get_actions_for_topic('step__beta__finished', msg))
    actions_3 = tuple(dequeuer.get_actions_for_topic('otherthing__created', msg))

    assert actions_1 == actions_2
    assert actions_1 != actions_3
