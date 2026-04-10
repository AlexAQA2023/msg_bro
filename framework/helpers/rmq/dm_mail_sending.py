from framework.internal.rmq.consumer_rmq import Consumer


class DmMailSending(Consumer):
    exchange = "dm.mail.sending"
    routing_key = "#"