// relogio fisico (com ajuste) e relogio logico de Lamport do processo
public static class Clock
{
    private static long offset = 0;   // quanto o relogio fisico foi ajustado (ms)
    private static long logical = 0;  // relogio logico (lamport)

    // hora atual em milissegundos (unix), ja com o ajuste
    public static long Now()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + offset;
    }

    public static void Adjust(long ms)
    {
        offset += ms;
    }

    // lamport: incrementa antes de enviar uma mensagem
    public static long Tick()
    {
        logical++;
        return logical;
    }

    // lamport: quando recebe, fica com o maior valor entre o seu e o da mensagem
    public static void Update(long received)
    {
        logical = Math.Max(logical, received);
    }

    public static string Format(long ms)
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(ms).ToString("HH:mm:ss.fff");
    }
}
