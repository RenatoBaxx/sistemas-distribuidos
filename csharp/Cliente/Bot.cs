public class Bot
{
    private const string BrokerAddress = "tcp://broker:5555";
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(5);

    // nomes que o bot usa quando precisa criar um canal
    private static readonly string[] ChannelNames =
    {
        "avisos", "duvidas", "projetos", "games", "futebol", "series", "random", "off-topic"
    };

    private readonly string name;
    private readonly Random random = new Random();

    public Bot(string name)
    {
        this.name = name;
    }

    public void Start()
    {
        Login();

        while (true)
        {
            var channels = ListChannels();
            if (channels.Count < 5)
                CreateChannel(channels);
            Thread.Sleep(3000);
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
