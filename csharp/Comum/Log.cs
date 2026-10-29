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
        sb.Append($"tipo: {m.Tipo} | de: {m.Remetente} | hora: {Clock.Format(m.Timestamp)}");
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
        return sb.ToString();
    }
}
