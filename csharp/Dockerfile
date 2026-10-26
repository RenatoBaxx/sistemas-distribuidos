FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# copia primeiro so os .csproj para o docker guardar o restore em cache
COPY csharp/Comum/Comum.csproj csharp/Comum/
COPY csharp/Servidor/Servidor.csproj csharp/Servidor/
COPY csharp/Cliente/Cliente.csproj csharp/Cliente/
RUN dotnet restore csharp/Servidor/Servidor.csproj
RUN dotnet restore csharp/Cliente/Cliente.csproj

COPY proto/ proto/
COPY csharp/ csharp/
RUN dotnet publish csharp/Servidor/Servidor.csproj -c Release -o /app/servidor --no-restore
RUN dotnet publish csharp/Cliente/Cliente.csproj -c Release -o /app/cliente --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
