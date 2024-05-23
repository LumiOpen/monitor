import pytest
from slurmmonitor.message import Message, MessageTracker

def test_message_initialization():
    msg = Message(topic="TestTopic", text="This is a test", details="Test details", active=True)
    assert msg.topic == "TestTopic"
    assert msg.text == "This is a test"
    assert msg.details == "Test details"
    assert msg.active

def test_message_str():
    msg_with_details = Message(topic="TestTopic", text="This is a test", details="Test details", active=True)
    assert str(msg_with_details) == "This is a test (Test details)"
    
    msg_without_details = Message(topic="TestTopic", text="This is a test", details="", active=True)
    assert str(msg_without_details) == "This is a test"

def test_tracker_initialization():
    tracker = MessageTracker()
    assert tracker.messages == {}

def test_handle_new_message():
    tracker = MessageTracker()
    msg = Message(topic="TestTopic", text="This is a test", details="Test details", active=True)
    result = tracker.handle(msg)
    assert tracker.messages["TestTopic"] == msg
    assert result == msg

def test_handle_existing_message():
    tracker = MessageTracker()
    msg1 = Message(topic="TestTopic", text="This is a test", details="Test details", active=True)
    tracker.handle(msg1)
    msg2 = Message(topic="TestTopic", text="Updated text", details="Updated details", active=True)
    result = tracker.handle(msg2)
    assert tracker.messages["TestTopic"] == msg2
    assert result == msg2

def test_handle_non_active_new_message():
    tracker = MessageTracker()
    msg = Message(topic="TestTopic", text="This is a test", details="Test details", active=False)
    result = tracker.handle(msg)
    assert tracker.messages["TestTopic"] == msg
    assert result is None

def test_get_active_messages():
    tracker = MessageTracker()
    msg1 = Message(topic="Topic1", text="Active message", details="Details", active=True)
    msg2 = Message(topic="Topic2", text="Inactive message", details="Details", active=False)
    msg3 = Message(topic="Topic3", text="Another active message", details="Details", active=True)
    tracker.handle(msg1)
    tracker.handle(msg2)
    tracker.handle(msg3)
    active_messages = tracker.get_active_messages()
    assert active_messages == [msg1, msg3]

def test_message_changes_state_active_to_inactive():
    tracker = MessageTracker()
    msg1 = Message(topic="TestTopic", text="This is a test", details="Test details", active=True)
    tracker.handle(msg1)
    msg2 = Message(topic="TestTopic", text="This is a new test", details="Test details", active=False)
    result = tracker.handle(msg2)
    assert tracker.messages["TestTopic"] == msg2
    assert result is msg2
    assert tracker.get_active_messages() == []

def test_message_changes_state_inactive_to_active():
    tracker = MessageTracker()
    msg1 = Message(topic="TestTopic", text="This is a test", details="Test details", active=False)
    tracker.handle(msg1)
    msg2 = Message(topic="TestTopic", text="This is a new test", details="Test details", active=True)
    result = tracker.handle(msg2)
    assert tracker.messages["TestTopic"] == msg2
    assert result == msg2
    assert tracker.get_active_messages() == [msg2]