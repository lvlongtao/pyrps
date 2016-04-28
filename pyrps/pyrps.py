from redis import StrictRedis

class PyRPS(object):
    """
    Python Publish/Subscribe client implemented over Redis with reliable
    message delivery to subscriber.

    The code is based on this blogpost, which should take all the credit
    for publishing the principe: https://davidmarquis.wordpress.com/2013/01/03/reliable-delivery-message-queues-with-redis/

    There is one global namespace, specified when constructing the PyRPS
    object, that will hold all keys needed for proper operation of PyRPS.
    It allows you to have multiple instances of PyRPS without affecting
    each other.

    Each PyRPS instance can accomodate multiple message queues. Each queue
    has zero to many subscribers, that gets messages.

    Each subscriber has it's own identifier so that multiple different
    subscribers can listen for messages in the same queue.

    You can connect multiple subscribers with same ID to the same queue,
    and in that case, only one of them will receive the message. It can be
    used to distribute work between multiple instances.

    The schema in REDIS is as follows:

    <namespace>.nextid                       INT  sequential counter of message IDs.
    <namespace>.<queue>.consumers            SET  of subscribed consumers
    <namespace>.<queue>.messages.<id>        STR  data of message
    <namespace>.<queue>.<consumer>.messages  LIST list of messages for given consumer.

    And the flowchart for namespace shop is as follows:

    Subscribing to message queue
    --------------------------------------------------------------------------

    sub = pyrps.subscribe("orders", "fullfillment")
        -->  SADD shop.orders.consumers fullfillment

    Publishing message
    --------------------------------------------------------------------------

    pyrps.publish("orders", "order data")
        -->  INCR shop.nextid
             -> 1

             SET shop.orders.messages.1 "order data"

             SMEMBERS shop.orders.consumers
             -> 1) invoicing
             -> 2) fullfillment

             RPUSH shop.orders.invoicing 1

             RPUSH shop.orders.fullfillment 1


    Reading message from queue
    --------------------------------------------------------------------------

    sub.consume()
        -->  BLPOP shop.orders.fullfillment
             -> 1

             GET shop.orders.messages.1
             -> "order data"

        <--  "order data"

    Destroying the message queue
    --------------------------------------------------------------------------

    sub.unsubscribe()
        -->  SREM shop.orders.consumers fullfillment

             DEL shop.orders.fullfillment.messages
    """

    def __init__(self, namespace, redis_url=("localhost", 6379)):
        """
        Create instance of PyRPS.
        @param redis_url Redis instance address (tuple containing (hostname, port)).
        @param namespace Namespace to separate Pub/Sub instance from another running on the same redis host.
        """

        self.namespace = namespace

        if isinstance(redis_url, tuple):
            self.redis = StrictRedis(host=redis_url[0], port=redis_url[1])
        elif isinstance(redis_url, str):
            self.redis = StrictRedis(host=redis_url)


    def subscribe(self, queue, consumer_id):
        """
        Subscribe to message queue. Yields messages as they appear in
        the queue.
        @param queue Queue name
        @param consumer_id Consumer name
        """

        # Add myself to the list of consumers, if not already present.
        self.redis.sadd(self._ns_subscriptions(queue), consumer_id)

        return Subscription(self, queue, consumer_id)


    def publish(self, queue, message, ttl=3600):
        """
        Publish new message into queue.
        @param queue Queue name.
        @param message Message data.
        @param ttl How long the message should stay alive.
        """

        # Get next message ID
        message_id = self.redis.incr(self._ns_nextid())

        # Push message to queue
        self.redis.setex(self._ns_message(queue, message_id), ttl, message)
        
        # List all consumers of given queue
        consumers = self.redis.smembers(self._ns_subscriptions(queue))

        # Publish the message to all the consumers.
        for consumer in consumers:
            self.redis.rpush(self._ns_queue(queue, consumer), message_id)


    def _ns(self, *args):
        """ Convinience method to retrieve names of redis keys including
        configured namespace. """
        return "%s.%s" % (self.namespace, ".".join([str(arg) for arg in args]))


    def _ns_subscriptions(self, queue):
        """ Return key for subscribers list for given queue. """
        return self._ns(queue, "consumers")


    def _ns_nextid(self):
        """ Return key for nextid counter. """
        return self._ns("nextid")


    def _ns_message(self, queue, message_id):
        """ Return key for retrieving message. """
        return self._ns(queue, "messages", message_id)


    def _ns_queue(self, queue, consumer_id):
        """ Return key for queue for one consumer. """
        return self._ns(queue, consumer_id, "messages")


class Subscription(object):
    """
    Object representing subscription to message queue.
    """

    def __init__(self, pyrps, queue, consumer_id):
        """
        Create instance of Subscription. Do not call directly, use
        pyrps.subscribe().
        """
        self.pyrps = pyrps
        self.queue = queue
        self.consumer_id = consumer_id


    def consume(self, block=True, timeout=0):
        """
        Wait for message to arrive and return it. Blocks if there is no
        message availalbe.
        @param block Wait for new message if there is no message in the queue. Default: True
           If block=False and there are no messages in the queue, this method returns None.
        @param timeout Maximum number of seconds to wait in blocking call. 0 (default) means wait indefinitely.
        @return Message data
        """

        # We need to repeat this step, because there may be messages
        # that expired, and we expect this method to always return message.
        while True:
            # Retrieve last message ID
            if block:
                # Blocking query, wait until message is available.
                message_id = self.pyrps.redis.blpop(self.pyrps._ns_queue(self.queue, self.consumer_id), timeout)

                # blpop returns tuple(key, value), we need only value.
                message_id = message_id[1]
            else:
                # Non blocking query. Return if there is no message in queue.
                message_id = self.pyrps.redis.lpop(self.pyrps._ns_queue(self.queue, self.consumer_id))

                # If there is no message in the queue, return None.
                if message_id is None:
                    return None

            # Retrieve the message
            message = self.pyrps.redis.get(self.pyrps._ns_message(self.queue, message_id))
            
            # If message still exists (no TTL has been reached), return it.
            if message is not None:
                return message


    def unsubscribe(self):
        """
        Unsubscribe from message queue and destroy it. Do not call if you
        want persistent queues or if you access one queue from multiple
        processes.
        """

        # Unsubscribe
        self.pyrps.redis.srem(self.pyrps._ns_subscriptions(self.queue), self.consumer_id) 

        # Remove message queue
        self.pyrps.redis.delete(self.pyrps._ns_queue(self.queue, self.consumer_id))

