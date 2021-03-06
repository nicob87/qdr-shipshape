from proton import Message
from basic import BasicCommon
import uuid
import sys
import logging


class BasicSender(BasicCommon):
    def __init__(self):
        super(BasicSender, self).__init__()
        # Sender specifics
        self._sender = None

    #
    # Internal methods
    #
    def can_send(self) -> bool:
        rd = self.result_data
        # Must be done first (to block a possible retry in case done sending)
        if self.done_sending():
            return False

        # Number of pending acks (sent - released - accepted)
        pendingacks = rd.delivered - rd.released - rd.accepted

        # Proceed only if accepted + pending < total
        # This avoids continue sending when we have pending acks
        return rd.accepted + pendingacks < self._msgcount

    def done_sending(self) -> bool:
        return self.result_data.accepted == self._msgcount

    #
    # Event handling
    #
    def on_start(self, event):
        self._sender = event.container.create_sender(self._url)

    def on_sendable(self, event):
        self.send(event, 'on_senable')

    def send(self, event, source):
        if not event.sender.credit or not self.can_send():
            logging.debug("[%s] unable to send - credit: %s - partial results: %s" % (source, event.sender.credit, self.result_data))
            return
        logging.debug("[%s] message sent: credit: %s - partial results: %s" % (source, event.sender.credit, self.result_data))
        msg = Message(id=str(uuid.uuid1()), body=self._expected_body)
        event.sender.send(msg)
        self.result_data.delivered += 1

    def on_accepted(self, event):
        logging.debug("message accepted: %s" % event.delivery.tag)
        self.result_data.accepted += 1

        if self.done_sending():
            logging.debug("done sending")
            event.sender.close()
            event.connection.close()


    def on_released(self, event):
        logging.debug("message released: %s" % event.delivery.tag)
        self.result_data.released += 1
        self.send(event, 'on_released')

    def on_rejected(self, event):
        logging.debug("message rejected: %s" % event.delivery.tag)
        self.result_data.rejected += 1
        self.send(event, 'on_released')

    def on_settled(self, event):
        logging.debug("message settled: %s" % event.delivery.tag)
        self.result_data.settled += 1


if __name__ == "__main__":
    BasicSender().execute_client()
