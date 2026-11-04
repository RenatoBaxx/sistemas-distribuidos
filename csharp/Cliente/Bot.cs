using System.Diagnostics;
using NetMQ;
using NetMQ.Sockets;

public class Bot
{
    private const string BrokerAddress = "tcp://broker:5555";
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(5);

    // nomes que o bot usa quando precisa criar um canal
    private static readonly string[] ChannelNames =
    {
        "avisos", "duvidas", "projetos", "games", "futebol", "series", "random", "off-topic"
    };

    // frases sorteadas para mandar nos canais
    private static readonly string[] Phrases =
    {
        "alguem online?", "boa tarde pessoal", "c# > python kkk", "to estudando pra prova",
        "que horas e a aula?", "mensagem de teste", "netmq funcionando", "bora jogar depois?",
        "alguem ja fez o lab?", "vlw galera"
    };

    private readonly string name;
    private readonly Random random = new Random();

    // recebe as mensagens dos canais em que o bot entrou (vem do proxy)
    private readonly SubscriberSocket subscriber = new SubscriberSocket();
    private readonly List<string> subscriptions = new List<string>();

    public Bot(string name)
    {
        this.name = name;
    }

    public void Start()
    {
        subscriber.Connect("tcp://proxy:5558");
        Login();

        while (true)
        {
            var channels = ListChannels();

            // regra 1: menos de 5 canais -> cria mais um
            if (channels.Count < 5)
            {
                CreateChannel(channels);
                channels = ListChannels();
            }

            // regra 2: inscrito em menos de 3 canais -> entra em mais um
            if (subscriptions.Count < 3)
                SubscribeOne(channels);

            // regra 3: escolhe um canal e manda 10 mensagens com 1 segundo de intervalo
            if (channels.Count == 0)
            {
                ReadChannels(TimeSpan.FromSeconds(1));
                continue;
            }
            var channel = channels[random.Next(channels.Count)];
            for (int i = 0; i < 10; i++)
            {
                Publish(channel, Phrases[random.Next(Phrases.Length)]);
                ReadChannels(TimeSpan.FromSeconds(1));
            }
        }
    }

    private void Login()
    {
        while (true)
        {
            var reply = Request(NewMessage("login"));
            if (reply?.Status == "ok")
                break;

            Log.Info("login falhou, tentando de novo em 1 segundo...");
            Thread.Sleep(1000);
        }
        Log.Info("login feito!");
    }

    private List<string> ListChannels()
    {
        var reply = Request(NewMessage("listar_canais"));
        if (reply == null || reply.Status != "ok")
            return new List<string>();
        return reply.Canais.ToList();
    }

    private void CreateChannel(List<string> existing)
    {
        var options = ChannelNames.Except(existing).ToArray();
        if (options.Length == 0)
            return;

        var request = NewMessage("criar_canal");
        request.Canal = options[random.Next(options.Length)];

        var reply = Request(request);
        if (reply?.Status == "erro")
            Log.Info($"nao consegui criar o canal {request.Canal} ({reply.Erro})");
    }

    private void SubscribeOne(List<string> channels)
    {
        var options = channels.Except(subscriptions).ToList();
        if (options.Count == 0)
            return;

        var channel = options[random.Next(options.Count)];
        subscriber.Subscribe(channel);
        subscriptions.Add(channel);
        Log.Info($"entrei no canal #{channel} (meus canais: {string.Join(", ", subscriptions)})");
    }

    private void Publish(string channel, string text)
    {
        var request = NewMessage("publicar");
        request.Canal = channel;
        request.Conteudo = text;

        var reply = Request(request);
        if (reply?.Status == "erro")
            Log.Info($"nao consegui publicar em #{channel} ({reply.Erro})");
    }

    // fica lendo as mensagens dos canais ate passar o tempo pedido
    private void ReadChannels(TimeSpan duration)
    {
        var timer = Stopwatch.StartNew();
        while (true)
        {
            var remaining = duration - timer.Elapsed;
            if (remaining <= TimeSpan.Zero)
                break;
            if (!subscriber.TryReceiveFrameString(remaining, out var topic))
                continue;
            var bytes = subscriber.ReceiveFrameBytes();

            // o filtro do zeromq e por prefixo, entao confere o nome inteiro do canal
            if (!subscriptions.Contains(topic))
                continue;

            var m = Mensagem.Parser.ParseFrom(bytes);
            Clock.Update(m.Relogio);
            Log.Info($"#{m.Canal} <{m.Remetente}> {m.Conteudo}  (enviada {Clock.Format(m.Timestamp)}, recebida {Clock.Format(Clock.Now())}, lamport {m.Relogio})");
        }
    }

    private Mensagem? Request(Mensagem request)
    {
        var reply = Net.Request(BrokerAddress, request, ReplyTimeout);
        if (reply == null)
            Log.Info("nenhum servidor respondeu a tempo");
        return reply;
    }

    private Mensagem NewMessage(string type)
    {
        return new Mensagem { Tipo = type, Remetente = name };
    }
}
