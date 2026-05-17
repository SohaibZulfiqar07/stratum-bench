FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY MiniPoolMonitor.sln ./
COPY MiniPoolMonitor/MiniPoolMonitor.Core/MiniPoolMonitor.Core.csproj MiniPoolMonitor/MiniPoolMonitor.Core/
COPY MiniPoolMonitor/MockMiner/MockMiner.csproj MiniPoolMonitor/MockMiner/

RUN dotnet restore MiniPoolMonitor.sln

COPY . .

# Publish self-contained so the app can run on the smaller runtime image even though it uses ASP.NET Core.
RUN dotnet publish MiniPoolMonitor/MiniPoolMonitor.Core/MiniPoolMonitor.Core.csproj \
  -c Release \
  -o /app/publish \
  -r linux-x64 \
  --self-contained true

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app

EXPOSE 3333
EXPOSE 5000

COPY --from=build /app/publish ./

# Default URLs (Program.cs also sets 0.0.0.0:5000, this keeps it explicit for containers)
ENV ASPNETCORE_URLS=http://0.0.0.0:5000

ENTRYPOINT ["./MiniPoolMonitor.Core"]
