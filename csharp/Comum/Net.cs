using Google.Protobuf;
using NetMQ;

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
}
