FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY proxy/Proxy.csproj proxy/
RUN dotnet restore proxy/Proxy.csproj

COPY proxy/ proxy/
RUN dotnet publish proxy/Proxy.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
CMD ["dotnet", "Proxy.dll"]
