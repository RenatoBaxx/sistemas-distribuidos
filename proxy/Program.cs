using NetMQ;
using NetMQ.Sockets;

// proxy do pub/sub: os servidores publicam na porta 5557 (XSUB) e os bots assinam na 5558 (XPUB)
using var xsub = new XSubscriberSocket();
xsub.Bind("tcp://*:5557");

using var xpub = new XPublisherSocket();
xpub.Bind("tcp://*:5558");

Console.WriteLine("proxy rodando: publicadores na porta 5557 e assinantes na porta 5558");

var proxy = new Proxy(xsub, xpub);
proxy.Start();
