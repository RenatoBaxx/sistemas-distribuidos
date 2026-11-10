using System.Text;

// mostra no console tudo que o programa envia e recebe
public static class Log
{
    public static string Name { get; set; } = "?";

    public static void Info(string text)
    {
        Console.WriteLine($"[{Name}] {text}");
    }

    public static void Sent(Mensagem m)
    {
        Console.WriteLine($"[{Name}] >> enviei  {Describe(m)}");
    }

    public static void Received(Mensagem m)
    {
        Console.WriteLine($"[{Name}] << recebi  {Describe(m)}");
    }

    private static string Describe(Mensagem m)
    {
        var sb = new StringBuilder();
        sb.Append($"tipo: {m.Tipo} | de: {m.Remetente} | hora: {Clock.Format(m.Timestamp)} | lamport: {m.Relogio}");
        if (m.Status != "")
            sb.Append($" | status: {m.Status}");
        if (m.Erro != "")
            sb.Append($" | erro: {m.Erro}");
        if (m.Canal != "")
            sb.Append($" | canal: {m.Canal}");
        if (m.Conteudo != "")
            sb.Append($" | texto: {m.Conteudo}");
        if (m.Canais.Count > 0)
            sb.Append($" | canais: [{string.Join(", ", m.Canais)}]");
        if (m.Rank > 0)
            sb.Append($" | rank: {m.Rank}");
        if (m.Servidores.Count > 0)
            sb.Append($" | servidores: [{string.Join(", ", m.Servidores.Select(s => $"{s.Nome}#{s.Rank}"))}]");
        if (m.Hora > 0)
            sb.Append($" | hora informada: {Clock.Format(m.Hora)}");
        if (m.Historico.Count == 1)
            sb.Append($" | registro: {DescribeRecord(m.Historico[0])}");
        else if (m.Historico.Count > 1)
            sb.Append($" | historico: {m.Historico.Count} registros");
        return sb.ToString();
    }

    // um registro da replicacao (o pedido original do bot que foi gravado)
    public static string DescribeRecord(Mensagem r)
    {
        var sb = new StringBuilder($"({r.Tipo} de {r.Remetente}");
        if (r.Canal != "")
            sb.Append($" canal: {r.Canal}");
        if (r.Conteudo != "")
            sb.Append($" texto: {r.Conteudo}");
        sb.Append($" lamport: {r.Relogio} id: {r.Id})");
        return sb.ToString();
    }
}
