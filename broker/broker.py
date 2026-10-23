import zmq

# Broker do request-reply: os bots conectam na porta 5555 e os servidores na 5556.
# O DEALER distribui os pedidos entre os servidores usando round-robin.

context = zmq.Context()

clientes = context.socket(zmq.ROUTER)
clientes.bind("tcp://*:5555")

servidores = context.socket(zmq.DEALER)
servidores.bind("tcp://*:5556")

print("broker rodando: clientes na porta 5555 e servidores na porta 5556")

zmq.proxy(clientes, servidores)
