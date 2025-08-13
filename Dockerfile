# --- Runtime image ---
FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim AS base
WORKDIR /app
# Ensure TLS root certs are present (needed for Event Hubs Kafka over SSL)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

# --- Build & publish ---
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
# Copy csproj and restore first for better layer caching
COPY ["chat-gpt-test.csproj", "./"]
RUN dotnet restore "chat-gpt-test.csproj"
# Copy the rest and publish (no self-contained apphost to keep image small)
COPY . .
RUN dotnet publish "chat-gpt-test.csproj" -c Release -o /app/out /p:UseAppHost=false

# --- Final image ---
FROM base AS final
WORKDIR /app
COPY --from=build /app/out ./
ENTRYPOINT ["dotnet", "chat-gpt-test.dll"]
