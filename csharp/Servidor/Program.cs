var name = Environment.GetEnvironmentVariable("NOME") ?? "servidor-cs";
Log.Name = name;

var server = new ChatServer(name);
server.Run();
