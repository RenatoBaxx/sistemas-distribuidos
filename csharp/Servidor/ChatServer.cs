using NetMQ;
using NetMQ.Sockets;

public class ChatServer
{
    private const string ReferenceAddress = "tcp://referencia:5559";
    private const int ServersPort = 5560;
    private static readonly TimeSpan ReferenceTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan ServerTimeout = TimeSpan.FromSeconds(2);

    // topicos do proxy usados entre os servidores (os bots nao podem usar como canal)
    private static readonly string[] Reserved = { "servers", "replicacao" };

    private readonly string name;
    private readonly Storage storage = new Storage("dados");
    private readonly List<string> channels = new List<string>();

    // replicacao: todos os dados (logins, canais e mensagens), deste e dos outros servidores
    private readonly List<Mensagem> records = new List<Mensagem>();
    // replicacao: ids dos registros que ja estao gravados
    private readonly HashSet<string> seen = new HashSet<string>();

    private int rank;
    private int clientMessages = 0;
    private string coordinator = "";

    // berkeley: diferenca do relogio de cada servidor para o do coordenador (so o coordenador usa)
    private readonly Dictionary<string, long> clockDiffs = new Dictionary<string, long>();

    // pedidos dos bots (chegam pelo broker)
    private readonly ResponseSocket clients = new ResponseSocket();

    // mensagens publicadas nos canais e no topico servers (vao para o proxy)
    private readonly PublisherSocket publisher = new PublisherSocket();

    // pedidos de outros servidores (eleicao, relogio e sincronizar)
    private readonly ResponseSocket servers = new ResponseSocket();

    // avisos de novo coordenador (topico servers) e replicas dos outros servidores (topico replicacao)
    private readonly SubscriberSocket serversTopic = new SubscriberSocket();

    public ChatServer(string name)
    {
        this.name = name;
        foreach (var record in storage.LoadAll())
            Remember(record);
        Log.Info($"{records.Count} registros e {channels.Count} canais carregados do disco");
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
        foreach (var topic in Reserved)
            serversTopic.Subscribe(topic);
        servers.Bind($"tcp://*:{ServersPort}");

        AskRank();
        var available = GetServers();
        Log.Info($"servidores disponiveis: {string.Join(", ", available.Select(s => $"{s.Nome} (rank {s.Rank})"))}");

        // espera o SUB dos topicos servers e replicacao conectar no proxy
        Thread.Sleep(1000);
        // assina a replicacao antes de pedir os dados, entao nada se perde no meio (o id evita duplicar)
        SyncData(available);
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
        if (topic == "replicacao" && announcement.Tipo == "replicacao")
        {
            ReceiveReplica(announcement);
            return;
        }
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
            case "sincronizar":
                Net.Send(servers, AnswerSync());
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

    // ---------------- replicacao (escrita em todas as replicas) ----------------

    private void Remember(Mensagem record)
    {
        records.Add(record);
        if (record.Id != "")
            seen.Add(record.Id);
        // o canal e identificado pelo nome: se dois servidores criaram o mesmo canal ao mesmo tempo,
        // os dois registros ficam guardados, mas o canal aparece uma vez so na lista
        if (record.Tipo == "criar_canal" && !channels.Contains(record.Canal))
            channels.Add(record.Canal);
    }

    // grava o registro se ele ainda nao existe aqui; devolve true se era novo
    private bool Apply(Mensagem record)
    {
        if (record.Id == "" || seen.Contains(record.Id) || !Storage.IsRecordType(record.Tipo))
            return false;
        storage.Save(record);
        Remember(record);
        return true;
    }

    // manda o registro novo para todos os servidores pelo topico replicacao do proxy
    private void Replicate(Mensagem record)
    {
        var envelope = NewMessage("replicacao");
        envelope.Historico.Add(record);
        Net.Publish(publisher, "replicacao", envelope);
    }

    private void ReceiveReplica(Mensagem envelope)
    {
        if (envelope.Remetente == name)
            return; // a propria replica volta pelo SUB
        foreach (var record in envelope.Historico)
            if (Apply(record))
                Log.Info($"gravei a replica de {envelope.Remetente}: {Log.DescribeRecord(record)}");
    }

    // quando o servidor liga ele pede tudo o que os outros tem (pode ter perdido coisas enquanto
    // estava desligado e o pub/sub nao guarda mensagens antigas)
    private void SyncData(List<InfoServidor> available)
    {
        var others = available.Where(s => s.Nome != name).ToList();
        if (others.Count == 0)
            Log.Info("nenhum outro servidor para sincronizar os dados");

        foreach (var server in others)
        {
            var reply = Net.Request($"tcp://{server.Nome}:{ServersPort}", NewMessage("sincronizar"), ServerTimeout);
            if (reply?.Status != "ok")
            {
                Log.Info($"{server.Nome} nao respondeu a sincronizacao");
                continue;
            }
            int added = reply.Historico.Count(Apply);
            Log.Info($"sincronizei com {server.Nome}: {added} registros novos de {reply.Historico.Count}");
        }
    }

    // resposta do sincronizar: todos os registros, na ordem do relogio logico (o id desempata)
    private Mensagem AnswerSync()
    {
        var reply = Ok("sincronizar");
        reply.Historico.AddRange(records
            .Where(r => r.Id != "")
            .OrderBy(r => r.Relogio)
            .ThenBy(r => r.Id, StringComparer.Ordinal));
        return reply;
    }

    private static Mensagem NewRecord(string type, Mensagem request, long timestamp)
    {
        return new Mensagem
        {
            Tipo = type,
            Remetente = request.Remetente,
            Timestamp = timestamp,
            Relogio = request.Relogio,
            Id = Guid.NewGuid().ToString("N")
        };
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

        var record = NewRecord("login", request, Clock.Now());
        Apply(record);
        Replicate(record);
        return Ok("login");
    }

    private Mensagem CreateChannel(Mensagem request)
    {
        string channel = request.Canal;

        if (!IsValidName(channel))
            return Error("criar_canal", "nome de canal invalido");
        if (Reserved.Contains(channel))
            return Error("criar_canal", $"o canal {channel} e reservado");
        if (channels.Contains(channel))
            return Error("criar_canal", "esse canal ja existe");

        var record = NewRecord("criar_canal", request, Clock.Now());
        record.Canal = channel;
        Apply(record);
        Replicate(record);

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

    // nao confere se o canal esta na lista porque um canal criado agora em outro servidor
    // pode ainda nao ter chegado aqui pela replicacao
    private Mensagem Publish(Mensagem request)
    {
        if (!IsValidName(request.Canal) || Reserved.Contains(request.Canal))
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
        var record = NewRecord("publicar", request, request.Timestamp);
        record.Canal = request.Canal;
        record.Conteudo = request.Conteudo;
        Apply(record);
        Replicate(record);

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
