using System.Text.Json;

// guarda os dados do servidor em arquivos .jsonl (um registro em json por linha).
// cada dado (login, canal ou mensagem) e um "registro": uma Mensagem com o pedido original
// do bot e um id unico, que e o que vai para o disco e para os outros servidores (parte 5)
public class Storage
{
    private static readonly JsonSerializerOptions jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    // arquivo onde cada tipo de registro e guardado
    private static readonly Dictionary<string, string> files = new Dictionary<string, string>
    {
        ["login"] = "logins.jsonl",
        ["criar_canal"] = "canais.jsonl",
        ["publicar"] = "mensagens.jsonl"
    };

    private readonly string folder;

    public Storage(string folder)
    {
        this.folder = folder;
        Directory.CreateDirectory(folder);
    }

    public static bool IsRecordType(string type)
    {
        return files.ContainsKey(type);
    }

    public void Save(Mensagem r)
    {
        switch (r.Tipo)
        {
            case "login":
                Append(files[r.Tipo], new LoginRecord(r.Remetente, r.Timestamp, r.Relogio, r.Id));
                break;
            case "criar_canal":
                Append(files[r.Tipo], new ChannelRecord(r.Canal, r.Remetente, r.Timestamp, r.Relogio, r.Id));
                break;
            case "publicar":
                Append(files[r.Tipo], new MessageRecord(r.Canal, r.Remetente, r.Conteudo, r.Timestamp, r.Relogio, r.Id));
                break;
        }
    }

    // le todos os registros do disco (dados salvos antes da parte 5 ficam com o id vazio)
    public List<Mensagem> LoadAll()
    {
        var all = new List<Mensagem>();

        foreach (var x in Read<LoginRecord>("login"))
            all.Add(new Mensagem { Tipo = "login", Remetente = x.Usuario ?? "", Timestamp = x.Timestamp, Relogio = x.Relogio, Id = x.Id ?? "" });

        foreach (var x in Read<ChannelRecord>("criar_canal"))
            all.Add(new Mensagem { Tipo = "criar_canal", Canal = x.Canal ?? "", Remetente = x.CriadoPor ?? "", Timestamp = x.Timestamp, Relogio = x.Relogio, Id = x.Id ?? "" });

        foreach (var x in Read<MessageRecord>("publicar"))
            all.Add(new Mensagem { Tipo = "publicar", Canal = x.Canal ?? "", Remetente = x.Usuario ?? "", Conteudo = x.Conteudo ?? "", Timestamp = x.Timestamp, Relogio = x.Relogio, Id = x.Id ?? "" });

        return all;
    }

    private IEnumerable<T> Read<T>(string type)
    {
        var path = Path.Combine(folder, files[type]);
        if (!File.Exists(path))
            return Enumerable.Empty<T>();
        return File.ReadLines(path).Select(line => JsonSerializer.Deserialize<T>(line, jsonOptions)!).ToList();
    }

    private void Append<T>(string file, T record)
    {
        var line = JsonSerializer.Serialize(record, jsonOptions);
        File.AppendAllText(Path.Combine(folder, file), line + "\n");
    }
}

public record LoginRecord(string? Usuario, long Timestamp, long Relogio, string? Id);
public record ChannelRecord(string? Canal, string? CriadoPor, long Timestamp, long Relogio, string? Id);
public record MessageRecord(string? Canal, string? Usuario, string? Conteudo, long Timestamp, long Relogio, string? Id);
