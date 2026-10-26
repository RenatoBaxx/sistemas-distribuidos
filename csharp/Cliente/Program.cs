var name = Environment.GetEnvironmentVariable("NOME") ?? "bot-cs";
Log.Name = name;

var bot = new Bot(name);
bot.Start();
