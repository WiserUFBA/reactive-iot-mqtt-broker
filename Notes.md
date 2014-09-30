Using Tree data structure for pub/sub:

- Tree is used only for pub/sub communication, if pub with QoS=2, store message to queue 
  and re-send when tree updates (when client do a subscription)

Publish:
1. locate tree node by msg.topic
2. get clients attached to tree node
3. for each client: publish/send message

Subscribe:
1. locate parent tree node (consider # and +) and update/append new tree nodes if necessary
sub /prova/+/dev/#

Ogni topic ha un array di messaggi (pubblicati) e un array di client (sottoscrizioni)
topic(no whildchar) --> messages
topic(no whildchar) --> clients
