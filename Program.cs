// Program.cs (net8.0)
// dotnet add package Confluent.Kafka

using System;
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// ---- Read config ----
string bootstrap = builder.Configuration["KAFKA_BOOTSTRAP"]      ?? throw new Exception("Missing KAFKA_BOOTSTRAP");
string username  = builder.Configuration["KAFKA_SASL_USERNAME"]  ?? "$ConnectionString";
string password  = builder.Configuration["KAFKA_SASL_PASSWORD"]  ?? throw new Exception("Missing KAFKA_SASL_PASSWORD");
string topic     = builder.Configuration["KAFKA_TOPIC"]          ?? throw new Exception("Missing KAFKA_TOPIC");
string group     = builder.Configuration["KAFKA_CONSUMER_GROUP"] ?? throw new Exception("Missing KAFKA_CONSUMER_GROUP");

// Base client config for Event Hubs Kafka endpoint
var baseConfig = new ClientConfig
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = username,
    SaslPassword = password,
    SocketTimeoutMs = 10000,
    // If you run in a minimal Linux container and hit SSL trust issues, uncomment:
    // SslCaLocation = "/etc/ssl/certs/ca-certificates.crt"
};

// --- Serve static dashboard (index.html) ---
app.UseDefaultFiles();  // serves wwwroot/index.html at "/"
app.UseStaticFiles();

// --- Metrics endpoint ---
app.MapGet("/metrics", () =>
{
    try
    {
        // 1) Discover partitions for the topic
        using var admin = new AdminClientBuilder(baseConfig).Build();
        var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
        var topicMd = md.Topics.FirstOrDefault();

        if (topicMd == null || topicMd.Error.IsError)
        {
            return Results.Json(new
            {
                error = $"Topic '{topic}' not found or metadata error",
                reason = topicMd?.Error.Reason
            }, statusCode: 404);
        }

        var partitions = topicMd.Partitions
            .Select(p => new TopicPartition(topic, new Partition(p.PartitionId)))
            .ToList();

        // 2) Read committed offsets for the target group
        var committedReaderCfg = new ConsumerConfig(baseConfig)
        {
            GroupId = group,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false
        };
        using var committedReader = new ConsumerBuilder<Ignore, Ignore>(committedReaderCfg).Build();

        var committedOffsetsList = committedReader.Committed(partitions, TimeSpan.FromSeconds(5));
        var committedOffsets = committedOffsetsList.ToDictionary(o => o.TopicPartition, o => o.Offset);

        // 3) Read end (high watermark) offsets using a short-lived monitor consumer (random group)
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
            // High watermark (the "next offset to be produced")
            var wm = monitor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
            long endOffset = wm.High == Offset.Unset ? 0 : wm.High.Value;
            endTotal += endOffset;

            // Committed offset = "next offset to consume" for the observed group
            committedOffsets.TryGetValue(tp, out var cmt);
            long committed = cmt == Offset.Unset ? 0 : cmt.Value;

            // Guard for hub recreation or stale offsets:
            if (committed > endOffset)
            {
                committed = 0; // or: committed = endOffset;
            }

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
            processedTotal, // ~events processed so far (sum of committed offsets)
            endTotal,       // ~events produced so far (sum of end offsets)
            perPartition
        });
    }
    catch (Exception ex)
    {
        return Results.Json(new { error = ex.GetType().Name, message = ex.Message }, statusCode: 500);
    }
});

app.Run();
