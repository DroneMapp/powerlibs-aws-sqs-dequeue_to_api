import pytest


def test_handle_message__patch(dequeuer, update_message):
    dequeuer.handle_message(update_message)

    method = dequeuer.mocked_requests_module.patch
    assert method.call_count == 1
    assert update_message.delete.call_count == 1


def test_handle_message__post(dequeuer, create_message):
    dequeuer.handle_message(create_message)

    method = dequeuer.mocked_requests_module.post
    assert method.call_count == 1
    assert create_message.delete.call_count == 1


def test_handle_message_without_necessary_key(dequeuer, born_message):
    with pytest.raises(KeyError):
        dequeuer.handle_message(born_message)
