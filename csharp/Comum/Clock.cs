public static class Clock
{
    // hora atual em milissegundos (unix)
    public static long Now()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public static string Format(long ms)
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(ms).ToString("HH:mm:ss.fff");
    }
}
