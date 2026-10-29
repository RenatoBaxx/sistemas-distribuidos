using System.Text.Json;

// guarda os dados do servidor em arquivos .jsonl (um registro em json por linha)
public class Storage
{
    private static readonly JsonSerializerOptions jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private readonly string folder;

    public Storage(string folder)
    {
        this.folder = folder;
        Directory.CreateDirectory(folder);
    }

    public void SaveLogin(string user, long timestamp)
    {
        Append("logins.jsonl", new LoginRecord(user, timestamp));
    }

    public void SaveChannel(string channel, string createdBy, long timestamp)
    {
        Append("canais.jsonl", new ChannelRecord(channel, createdBy, timestamp));
    }

    public void SaveMessage(string channel, string user, string text, long timestamp)
    {
        Append("mensagens.jsonl", new MessageRecord(channel, user, text, timestamp));
    }

    public List<string> LoadChannels()
    {
        var path = Path.Combine(folder, "canais.jsonl");
        if (!File.Exists(path))
            return new List<string>();

        return File.ReadLines(path)
            .Select(line => JsonSerializer.Deserialize<ChannelRecord>(line, jsonOptions)!.Canal)
            .ToList();
    }

    private void Append<T>(string file, T record)
    {
        var line = JsonSerializer.Serialize(record, jsonOptions);
        File.AppendAllText(Path.Combine(folder, file), line + "\n");
    }
}

public record LoginRecord(string Usuario, long Timestamp);
public record ChannelRecord(string Canal, string CriadoPor, long Timestamp);
public record MessageRecord(string Canal, string Usuario, string Conteudo, long Timestamp);
