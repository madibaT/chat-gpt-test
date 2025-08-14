// Program.cs (net8.0)
// dotnet add package Confluent.Kafka
//
// Purpose:
//   Compute Kafka-style lag on Azure Event Hubs by comparing the consumer group's
//   committed offsets to each partition's end (high watermark) offsets.
//   Serves JSON at /metrics and (optionally) a static dashboard from wwwroot/.
//
// Environment variables:
//   KAFKA_BOOTSTRAP       = <namespace>.servicebus.windows.net:9093
//   KAFKA_SASL_USERNAME   = $ConnectionString
//   KAFKA_SASL_PASSWORD   = Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...
//   KAFKA_TOPIC           = <event-hub-name>
//   KAFKA_CONSUMER_GROUP  = <consumer-group-to-measure>
//
// Note on hub re-creation:
//   If you delete & recreate the hub, committed offsets might exceed the new end offsets.
//   To protect visuals without wiping progress, we CLAMP: committed = Math.Min(committed, endOffset).

using System;
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

string bootstrap = builder.Configuration["KAFKA_BOOTSTRAP"] ?? throw new Exception("Missing KAFKA_BOOTSTRAP");
string username  = builder.Configuration["KAFKA_SASL_USERNAME"] ?? "$ConnectionString";
string password  = builder.Configuration["KAFKA_SASL_PASSWORD"] ?? throw new Exception("Missing KAFKA_SASL_PASSWORD");
string topic     = builder.Configuration["KAFKA_TOPIC"] ?? throw new Exception("Missing KAFKA_TOPIC");
string group     = builder.Configuration["KAFKA_CONSUMER_GROUP"] ?? throw new Exception("Missing KAFKA_CONSUMER_GROUP");

// Base config for Event Hubs Kafka endpoint
var baseConfig = new ClientConfig
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = username,
    SaslPassword = password,
    SocketTimeoutMs = 10000
    // If you run in a minimal Linux container and hit SSL trust issues, you may need:
    // SslCaLocation = "/etc/ssl/certs/ca-certificates.crt"
};

// Serve dashboard if present (wwwroot/index.html at "/")
app.UseDefaultFiles();
app.UseStaticFiles();

// Fallback note if you don't have a static page
app.MapGet("/", () => "OK. GET /metrics for JSON lag metrics.");

// Synchronous endpoints (Confluent client is synchronous here)
app.MapGet("/metrics", () =>
{
    // 1) Discover partitions (metadata)
    using var admin = new AdminClientBuilder(baseConfig).Build();
    var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
    if (md.Topics.Count == 0 || md.Topics[0].Error.IsError)
        return Results.Json(new { error = $"Topic '{topic}' not found or metadata error: {md.Topics.FirstOrDefault()?.Error.Reason}" }, statusCode: 404);

    var partitions = md.Topics[0].Partitions
        .Select(p => new TopicPartition(topic, new Partition(p.PartitionId)))
        .ToList();

    // 2) Build a consumer IN THE TARGET GROUP to read that group's committed offsets
    var committedReaderCfg = new ConsumerConfig(baseConfig)
    {
        GroupId = group,
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false
    };
    using var committedReader = new ConsumerBuilder<Ignore, Ignore>(committedReaderCfg).Build();

    var committedOffsets = committedReader.Committed(partitions, TimeSpan.FromSeconds(5))
        .ToDictionary(o => o.TopicPartition, o => o.Offset);

    // 3) Short-lived monitor consumer (random group) to read end (high watermark) offsets
    var monitorCfg = new ConsumerConfig(baseConfig)
    {
        GroupId = $"monitor-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false
    };
    using var monitor = new ConsumerBuilder<Ignore, Ignore>(monitorCfg).Build();

    long totalLag = 0;
    long processedTotal = 0;
    long endTotal = 0;
    var perPartition = new List<object>();

    foreach (var tp in partitions)
    {
        // High watermark = "next offset to be produced"
        var wm = monitor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
        long endOffset = wm.High == Offset.Unset ? 0 : wm.High.Value;
        endTotal += endOffset;

        // Committed offset for the target group = "next offset to consume"
        committedOffsets.TryGetValue(tp, out var cmt);
        long committed = cmt == Offset.Unset ? 0 : cmt.Value;

        // ---- SAFE GUARD FOR HUB RE-CREATION ----
        // Keep processed numbers reasonable without zeroing valid commits:
        committed = Math.Min(committed, endOffset);

        long lag = Math.Max(0, endOffset - committed);
        totalLag += lag;
        processedTotal += committed;

        perPartition.Add(new
        {
            partition = tp.Partition.Value,
            committed,
            end = endOffset,
            lag
        });
    }

    return Results.Json(new
    {
        topic,
        consumerGroup = group,
        totalLag,
        processedTotal,   // ~total processed so far (sum of committed offsets, clamped to end)
        endTotal,         // ~total produced so far (sum of end offsets)
        perPartition
    });
});

app.Run();
