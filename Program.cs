// Program.cs (net8.0)
// dotnet add package Confluent.Kafka

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
};

app.MapGet("/", () => "GET /metrics for JSON lag metrics.");

// NOTE: not async — all calls below are synchronous in the Confluent client.
app.MapGet("/metrics", () =>
{
    // 1) Discover partitions (metadata)
    using var admin = new AdminClientBuilder(baseConfig).Build();
    var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
    if (md.Topics.Count == 0 || md.Topics[0].Error.IsError)
        return Results.Json(new { error = $"Topic '{topic}' not found or metadata error: {md.Topics.FirstOrDefault()?.Error.Reason}" });

    var partitions = md.Topics[0].Partitions
        .Select(p => new TopicPartition(topic, new Partition(p.PartitionId)))
        .ToList();

    // 2) Build a consumer **in the target group** to read that group's committed offsets
    var committedReaderCfg = new ConsumerConfig(baseConfig)
    {
        GroupId = group,
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false
    };
    using var committedReader = new ConsumerBuilder<Ignore, Ignore>(committedReaderCfg).Build();

    var committedOffsets = committedReader.Committed(partitions, TimeSpan.FromSeconds(5))
        .ToDictionary(o => o.TopicPartition, o => o.Offset);

    // 3) Separate short-lived monitor consumer (random group) to read END (high) offsets
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
        processedTotal += committed;

        long lag = Math.Max(0, endOffset - committed);
        totalLag += lag;

        perPartition.Add(new { partition = tp.Partition.Value, committed, end = endOffset, lag });
    }

    return Results.Json(new
    {
        topic,
        consumerGroup = group,
        totalLag,
        processedTotal,   // ~total messages processed so far (sum of committed offsets)
        endTotal,         // ~total produced so far (sum of end offsets)
        perPartition
    });
});

app.UseDefaultFiles();  // serves wwwroot/index.html at "/"
app.UseStaticFiles();
app.Run();
