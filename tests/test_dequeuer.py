def test_handle_message__patch(dequeuer, update_message):
    dequeuer.handle_message(update_message)

    assert dequeuer.request_methods['patch'].call_count == 1
    assert update_message.delete.call_count == 1


def test_handle_message__post(dequeuer, create_message):
    dequeuer.handle_message(create_message)

    assert dequeuer.request_methods['post'].call_count == 1
    assert create_message.delete.call_count == 1
