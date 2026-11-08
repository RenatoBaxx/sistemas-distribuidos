using NetMQ;
using NetMQ.Sockets;

public class ChatServer
{
    private const string ReferenceAddress = "tcp://referencia:5559";
    private const int ServersPort = 5560;
    private static readonly TimeSpan ReferenceTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan ServerTimeout = TimeSpan.FromSeconds(2);

    private readonly string name;
    private readonly Storage storage = new Storage("dados");
    private readonly List<string> channels;

    private int rank;
    private int clientMessages = 0;
    private string coordinator = "";

    // berkeley: diferenca do relogio de cada servidor para o do coordenador (so o coordenador usa)
    private readonly Dictionary<string, long> clockDiffs = new Dictionary<string, long>();

    // pedidos dos bots (chegam pelo broker)
    private readonly ResponseSocket clients = new ResponseSocket();

    // mensagens publicadas nos canais e no topico servers (vao para o proxy)
    private readonly PublisherSocket publisher = new PublisherSocket();

    // pedidos de outros servidores (eleicao e relogio)
    private readonly ResponseSocket servers = new ResponseSocket();

    // avisos de novo coordenador (topico servers)
    private readonly SubscriberSocket serversTopic = new SubscriberSocket();

    public ChatServer(string name)
    {
        this.name = name;
        channels = storage.LoadChannels();
        Log.Info($"{channels.Count} canais carregados do disco");
    }

    public void Run()
    {
        // todos os containers usam o relogio do mesmo computador, entao cada servidor
        // comeca com um erro aleatorio para dar para ver a sincronizacao funcionando
        var drift = new Random().Next(-3000, 3001);
        Clock.Adjust(drift);
        Log.Info($"relogio fisico comecando com {drift} ms de diferenca");

        publisher.Connect("tcp://proxy:5557");
        serversTopic.Connect("tcp://proxy:5558");
        serversTopic.Subscribe("servers");
        servers.Bind($"tcp://*:{ServersPort}");

        AskRank();
        ShowServers();

        // espera o SUB do topico servers conectar no proxy antes da primeira eleicao
        Thread.Sleep(1000);
        StartElection();

        clients.Connect("tcp://broker:5556");
        Log.Info("conectado no broker, esperando pedidos...");

        // o poller atende os 3 sockets na mesma thread (os sockets do NetMQ nao sao thread-safe)
        clients.ReceiveReady += (s, e) => OnClientRequest();
        servers.ReceiveReady += (s, e) => OnServerRequest();
        serversTopic.ReceiveReady += (s, e) => OnServersTopic();

        using var poller = new NetMQPoller { clients, servers, serversTopic };
        poller.Run();
    }

    private void OnClientRequest()
    {
        var request = Net.Receive(clients);
        var reply = Handle(request);
        Net.Send(clients, reply);

        clientMessages++;
        if (clientMessages % 10 == 0)
            SendHeartbeat();
        if (clientMessages % 15 == 0)
            SyncClock();
    }

    // ---------------- servico de referencia ----------------

    private void AskRank()
    {
        while (true)
        {
            var reply = Net.Request(ReferenceAddress, NewMessage("rank"), ReferenceTimeout);
            if (reply?.Status == "ok")
            {
                rank = reply.Rank;
                Log.Info($"meu rank e {rank}");
                return;
            }
            Log.Info("a referencia nao respondeu, tentando de novo...");
            Thread.Sleep(1000);
        }
    }

    private List<InfoServidor> GetServers()
    {
        var reply = Net.Request(ReferenceAddress, NewMessage("listar_servidores"), ReferenceTimeout);
        if (reply == null || reply.Status != "ok")
            return new List<InfoServidor>();
        return reply.Servidores.ToList();
    }

    private void ShowServers()
    {
        var list = GetServers().Select(s => $"{s.Nome} (rank {s.Rank})");
        Log.Info($"servidores disponiveis: {string.Join(", ", list)}");
    }

    private void SendHeartbeat()
    {
        var reply = Net.Request(ReferenceAddress, NewMessage("heartbeat"), ReferenceTimeout);
        if (reply == null)
            Log.Info("a referencia nao respondeu o heartbeat");
        else if (reply.Status == "erro")
            AskRank(); // a referencia reiniciou e nao conhece mais este servidor
    }

    // ---------------- eleicao (algoritmo do valentao / bully) ----------------

    private void StartElection()
    {
        Log.Info("comecando eleicao");
        var higher = GetServers().Where(s => s.Rank > rank).ToList();

        bool someoneAnswered = false;
        foreach (var server in higher)
        {
            var reply = Net.Request($"tcp://{server.Nome}:{ServersPort}", NewMessage("eleicao"), ServerTimeout);
            if (reply?.Status == "ok")
                someoneAnswered = true;
        }

        if (someoneAnswered)
        {
            // um servidor com rank maior vai assumir e avisar no topico servers
            coordinator = "";
            Log.Info("tem servidor com rank maior vivo, esperando o aviso do novo coordenador");
            return;
        }

        coordinator = name;
        clockDiffs.Clear();

        var announcement = NewMessage("coordenador");
        announcement.Conteudo = name;
        announcement.Rank = rank;
        Net.Publish(publisher, "servers", announcement);
        Log.Info("ganhei a eleicao, agora eu sou o coordenador");
    }

    private void OnServersTopic()
    {
        var topic = serversTopic.ReceiveFrameString();
        var announcement = Net.Read(serversTopic.ReceiveFrameBytes());
        if (topic != "servers" || announcement.Tipo != "coordenador")
            return;

        coordinator = announcement.Conteudo;
        Log.Info($"novo coordenador: {coordinator}");

        // bully: se quem ganhou tem rank menor que o meu, eu tenho que assumir
        if (announcement.Rank < rank)
            StartElection();
    }

    // pedidos que chegam de outros servidores pela porta 5560
    private void OnServerRequest()
    {
        var request = Net.Receive(servers);
        switch (request.Tipo)
        {
            case "eleicao":
                // quem pediu tem rank menor: responde OK e continua a eleicao daqui
                Net.Send(servers, Ok("eleicao"));
                StartElection();
                break;
            case "relogio":
                Net.Send(servers, AnswerClock(request));
                break;
            default:
                Net.Send(servers, Error(request.Tipo, "nao conheco esse tipo de mensagem"));
                break;
        }
    }

    // berkeley (parte do coordenador): guarda a diferenca do relogio de quem pediu, faz a media
    // de todas as diferencas (o coordenador conta como 0) e devolve a hora ja na media
    private Mensagem AnswerClock(Mensagem request)
    {
        if (coordinator != name)
            return Error("relogio", "eu nao sou o coordenador");

        clockDiffs[request.Remetente] = request.Hora - Clock.Now();
        long average = clockDiffs.Values.Sum() / (clockDiffs.Count + 1);

        // o coordenador tambem anda ate a media, entao as diferencas guardadas mudam junto
        Clock.Adjust(average);
        foreach (var server in clockDiffs.Keys.ToList())
            clockDiffs[server] -= average;

        // quem pediu vai usar a hora da resposta, entao fica sem diferenca
        clockDiffs[request.Remetente] = 0;

        var reply = Ok("relogio");
        reply.Hora = Clock.Now();
        return reply;
    }

    // berkeley (parte de quem pede): manda a sua hora para o coordenador e usa a hora da resposta.
    // se o coordenador nao responder, considera que ele caiu e faz uma eleicao
    private void SyncClock()
    {
        if (coordinator == "")
        {
            StartElection();
            return;
        }
        if (coordinator == name)
            return;

        var request = NewMessage("relogio");
        request.Hora = Clock.Now();
        var reply = Net.Request($"tcp://{coordinator}:{ServersPort}", request, ServerTimeout);

        if (reply == null || reply.Status != "ok")
        {
            Log.Info($"o coordenador {coordinator} nao respondeu, comecando nova eleicao");
            coordinator = "";
            StartElection();
            return;
        }

        long diff = reply.Hora - Clock.Now();
        Clock.Adjust(diff);
        Log.Info($"relogio fisico ajustado em {diff} ms pelo coordenador {coordinator}");
    }

    // ---------------- pedidos dos bots ----------------

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
        storage.SaveMessage(request.Canal, request.Remetente, request.Conteudo, request.Timestamp, request.Relogio);

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

    private Mensagem NewMessage(string type)
    {
        return new Mensagem { Tipo = type, Remetente = name };
    }
}
