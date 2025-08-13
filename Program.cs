// Program.cs (net8.0)
// dotnet add package Confluent.Kafka

using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

/*
  Required env/appsettings:
    KAFKA_BOOTSTRAP          = <namespace>.servicebus.windows.net:9093
    KAFKA_SASL_USERNAME      = $ConnectionString
    KAFKA_SASL_PASSWORD      = Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...   (Event Hubs conn string)
    KAFKA_TOPIC              = <your event hub name>
    KAFKA_CONSUMER_GROUP     = <the consumer group you want to measure>
*/

string bootstrap = app.Configuration["KAFKA_BOOTSTRAP"] ?? throw new Exception("Missing KAFKA_BOOTSTRAP");
string username  = app.Configuration["KAFKA_SASL_USERNAME"] ?? "$ConnectionString";
string password  = app.Configuration["KAFKA_SASL_PASSWORD"] ?? throw new Exception("Missing KAFKA_SASL_PASSWORD");
string topic     = app.Configuration["KAFKA_TOPIC"] ?? throw new Exception("Missing KAFKA_TOPIC");
string group     = app.Configuration["KAFKA_CONSUMER_GROUP"] ?? throw new Exception("Missing KAFKA_CONSUMER_GROUP");

// Shared client config for Event Hubs Kafka endpoint
var baseConfig = new ClientConfig
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = username,
    SaslPassword = password,
    // If running in a very locked-down Linux container, you might need: SslCaLocation = "/etc/ssl/certs/ca-certificates.crt"
};

// Serve a super-small HTML page (optional)
// Create wwwroot/index.html if you want a chart; otherwise remove this.
app.MapGet("/", async ctx =>
{
    ctx.Response.ContentType = "text/plain";
    await ctx.Response.WriteAsync("GET /metrics for JSON lag metrics.");
});

// Core metrics endpoint
app.MapGet("/metrics", async () =>
{
    using var admin = new AdminClientBuilder(baseConfig).Build();

    // 1) Find partitions
    var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
    if (md.Topics.Count == 0 || md.Topics[0].Error.IsError)
        return Results.Json(new { error = $"Topic '{topic}' not found or metadata error." });

    var partitions = md.Topics[0].Partitions.Select(p => new TopicPartition(topic, new Partition(p.PartitionId))).ToList();

    // 2) Read committed offsets for the *target* consumer group
    var committed = admin.ListConsumerGroupOffsets(group, partitions);

    // 3) Read END (high watermark) offsets using a short-lived monitor consumer
    var monitorConfig = new ConsumerConfig(baseConfig)
    {
        GroupId = $"monitor-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 10000,
        SessionTimeoutMs = 10000
    };
    using var monitor = new ConsumerBuilder<Ignore, Ignore>(monitorConfig).Build();

    long totalLag = 0;
    var perPartition = new List<object>();

    foreach (var tp in partitions)
    {
        // End offsets (low/high). High is "next offset to be produced".
        var watermarks = monitor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
        long endOffset = watermarks.High.Value; // already "next" offset

        // Committed offset for the measured group
        // If never committed, Offset.Unset -> treat as 0
        var cmt = committed.Offsets.FirstOrDefault(o => o.TopicPartition == tp).Offset;
        long committedOffset = cmt == Offset.Unset ? 0 : cmt.Value;

        long lag = Math.Max(0, endOffset - committedOffset);
        totalLag += lag;

        perPartition.Add(new
        {
            partition = tp.Partition.Value,
            committed = committedOffset,
            end = endOffset,
            lag
        });
    }

    return Results.Json(new { topic, consumerGroup = group, totalLag, perPartition });
});

app.UseDefaultFiles();  // serves wwwroot/index.html at "/"
app.UseStaticFiles();
app.Run();
