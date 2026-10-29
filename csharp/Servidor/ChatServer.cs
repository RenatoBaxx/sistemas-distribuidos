using NetMQ.Sockets;

public class ChatServer
{
    private readonly string name;
    private readonly Storage storage = new Storage("dados");
    private readonly List<string> channels;

    // pedidos dos bots (chegam pelo broker)
    private readonly ResponseSocket clients = new ResponseSocket();

    // mensagens publicadas nos canais (vao para o proxy)
    private readonly PublisherSocket publisher = new PublisherSocket();

    public ChatServer(string name)
    {
        this.name = name;
        channels = storage.LoadChannels();
        Log.Info($"{channels.Count} canais carregados do disco");
    }

    public void Run()
    {
        publisher.Connect("tcp://proxy:5557");
        clients.Connect("tcp://broker:5556");
        Log.Info("conectado no broker, esperando pedidos...");

        while (true)
            OnClientRequest();
    }

    private void OnClientRequest()
    {
        var request = Net.Receive(clients);
        var reply = Handle(request);
        Net.Send(clients, reply);
    }

    private Mensagem Handle(Mensagem request)
    {
        return request.Tipo switch
        {
            "login" => Login(request),
            "criar_canal" => CreateChannel(request),
            "listar_canais" => ListChannels(),
            "publicar" => Publish(request),
            _ => Error(request.Tipo, "nao conheco esse tipo de mensagem"),
        };
    }

    private Mensagem Login(Mensagem request)
    {
        if (!IsValidName(request.Remetente))
            return Error("login", "nome de usuario invalido");

        storage.SaveLogin(request.Remetente, Clock.Now());
        return Ok("login");
    }

    private Mensagem CreateChannel(Mensagem request)
    {
        string channel = request.Canal;

        if (!IsValidName(channel))
            return Error("criar_canal", "nome de canal invalido");
        if (channel == "servers")
            return Error("criar_canal", "o canal servers e reservado");
        if (channels.Contains(channel))
            return Error("criar_canal", "esse canal ja existe");

        channels.Add(channel);
        storage.SaveChannel(channel, request.Remetente, Clock.Now());

        var reply = Ok("criar_canal");
        reply.Canal = channel;
        return reply;
    }

    private Mensagem ListChannels()
    {
        var reply = Ok("listar_canais");
        reply.Canais.AddRange(channels);
        return reply;
    }

    // nao confere se o canal esta na lista porque, com o round-robin, cada servidor so conhece parte dos canais
    private Mensagem Publish(Mensagem request)
    {
        if (!IsValidName(request.Canal) || request.Canal == "servers")
            return Error("publicar", "nome de canal invalido");
        if (string.IsNullOrWhiteSpace(request.Conteudo))
            return Error("publicar", "a mensagem esta vazia");

        // na publicacao o remetente e o bot que escreveu
        var post = new Mensagem
        {
            Tipo = "publicacao",
            Remetente = request.Remetente,
            Canal = request.Canal,
            Conteudo = request.Conteudo
        };
        Net.Publish(publisher, request.Canal, post);
        storage.SaveMessage(request.Canal, request.Remetente, request.Conteudo, request.Timestamp);

        var reply = Ok("publicar");
        reply.Canal = request.Canal;
        return reply;
    }

    // so letras minusculas, numeros, '-' e '_', com no maximo 20 caracteres
    private static bool IsValidName(string text)
    {
        if (text.Length == 0 || text.Length > 20)
            return false;
        return text.All(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_');
    }

    private Mensagem Ok(string type)
    {
        return new Mensagem { Tipo = type, Remetente = name, Status = "ok" };
    }

    private Mensagem Error(string type, string error)
    {
        return new Mensagem { Tipo = type, Remetente = name, Status = "erro", Erro = error };
    }
}
