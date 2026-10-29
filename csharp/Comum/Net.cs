using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

// envio e recebimento da Mensagem (protobuf) pelos sockets do NetMQ
public static class Net
{
    public static void Send(IOutgoingSocket socket, Mensagem m)
    {
        m.Timestamp = Clock.Now();
        Log.Sent(m);
        socket.SendFrame(m.ToByteArray());
    }

    public static Mensagem Receive(IReceivingSocket socket)
    {
        return Read(socket.ReceiveFrameBytes());
    }

    public static Mensagem Read(byte[] bytes)
    {
        var m = Mensagem.Parser.ParseFrom(bytes);
        Log.Received(m);
        return m;
    }

    // publicacao em 2 partes: o topico (nome do canal) e depois a mensagem
    public static void Publish(PublisherSocket socket, string topic, Mensagem m)
    {
        m.Timestamp = Clock.Now();
        Log.Sent(m);
        socket.SendMoreFrame(topic).SendFrame(m.ToByteArray());
    }

    // usa um REQ novo para cada pedido; se ninguem responder a tempo devolve null
    public static Mensagem? Request(string address, Mensagem m, TimeSpan timeout)
    {
        using var socket = new RequestSocket();
        socket.Options.Linger = TimeSpan.Zero;
        socket.Connect(address);
        Send(socket, m);

        if (socket.TryReceiveFrameBytes(timeout, out var bytes))
            return Read(bytes);
        return null;
    }
}
