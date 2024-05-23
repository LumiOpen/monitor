class Message:
    def __init__(self, topic, text, details, active=True):
        self.topic = topic
        self.text = text
        self.details = details
        self.active = active

    def __str__(self):
        if self.details:
            return f"{self.text} ({self.details})"
        return self.text

    def __repr__(self):
        return str(self)


# MessageTracker enables us to determine which messages are new and need to be
# reported.
class MessageTracker:
    def __init__(self):
        self.messages = {}

    def get_active_messages(self):
        return [i for i in self.messages.values() if i.active]

    def handle(self, message):
        orig = self.messages.get(message.topic, None)
        self.messages[message.topic] = message

        if not orig and not message.active:
            # non-active messages tend to be like 'condition cleared' and we don't
            # want to report these unless the condition was there in the first place.
            return

        # TODO what if the message is the same message, but the active state differs? hmm...
        if not orig or orig.text != message.text:
            return message
