def test_topic_regexp_matching(dequeuer):
    msg = {'company_name': 'test_company'}
    actions_1 = tuple(dequeuer.get_actions_for_topic('object__created', msg))
    actions_2 = tuple(dequeuer.get_actions_for_topic('object__deleted', msg))
    actions_3 = tuple(dequeuer.get_actions_for_topic('otherthing__created', msg))

    assert actions_1 == actions_2
    assert actions_1 != actions_3
