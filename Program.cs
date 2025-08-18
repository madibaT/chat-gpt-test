// Program.cs (net8.0)
// dotnet add package Confluent.Kafka
//
// Computes Kafka-style lag on Azure Event Hubs and adds "watermark delay":
//   delay = producedTimestamp(endOffset-1) - processedTimestamp(committed-1)
// Uses a short-lived monitor consumer to read watermarks and message timestamps.
//
// Env vars:
//   KAFKA_BOOTSTRAP       = <namespace>.servicebus.windows.net:9093
//   KAFKA_SASL_USERNAME   = $ConnectionString
//   KAFKA_SASL_PASSWORD   = Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...
//   KAFKA_TOPIC           = <event-hub-name>
//   KAFKA_CONSUMER_GROUP  = <consumer-group-to-measure>

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

var baseConfig = new ClientConfig
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = username,
    SaslPassword = password,
    SocketTimeoutMs = 10000
    // If needed in minimal containers:
    // SslCaLocation = "/etc/ssl/certs/ca-certificates.crt"
};

// Serve dashboard if present (wwwroot/index.html at "/")
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/metrics", () =>
{
    // Helper to read message timestamp at a specific absolute offset (returns null if not found).
    DateTimeOffset? ReadTimestampAt(IConsumer<Ignore, Ignore> cons, TopicPartition tp, long absoluteOffset)
    {
        try
        {
            if (absoluteOffset < 0) return null;
            cons.Assign(new TopicPartitionOffset(tp, new Offset(absoluteOffset)));
            var cr = cons.Consume(TimeSpan.FromSeconds(3));
            cons.Unassign();
            if (cr != null && cr.Message != null)
            {
                // Kafka timestamps are in UTC
                return cr.Message.Timestamp.UtcDateTime;
            }
        }
        catch { /* ignore and return null */ }
        return null;
    }

    try
    {
        // 1) Discover partitions
        using var admin = new AdminClientBuilder(baseConfig).Build();
        var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
        var topicMd = md.Topics.FirstOrDefault();
        if (topicMd == null || topicMd.Error.IsError)
            return Results.Json(new { error = $"Topic '{topic}' not found", reason = topicMd?.Error.Reason }, statusCode: 404);

        var partitions = topicMd.Partitions
            .Select(p => new TopicPartition(topic, new Partition(p.PartitionId)))
            .ToList();

        // 2) Read committed offsets (next-to-consume) for observed group
        var committedCfg = new ConsumerConfig(baseConfig)
        {
            GroupId = group,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false
        };
        using var committedReader = new ConsumerBuilder<Ignore, Ignore>(committedCfg).Build();
        var committedMap = committedReader.Committed(partitions, TimeSpan.FromSeconds(5))
            .ToDictionary(o => o.TopicPartition, o => o.Offset);

        // 3) Monitor consumer to read end offsets AND point-fetch timestamps
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

        var delays = new List<long>(); // ms
        var perPartition = new List<object>();

        foreach (var tp in partitions)
        {
            // End (next-to-produce) = high watermark
            var wm = monitor.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
            long endOffset = wm.High == Offset.Unset ? 0 : wm.High.Value;
            endTotal += endOffset;

            // Committed (next-to-consume)
            committedMap.TryGetValue(tp, out var cmt);
            long committed = cmt == Offset.Unset ? 0 : cmt.Value;

            // Clamp committed to not exceed endOffset (protect after hub re-create)
            committed = Math.Min(committed, endOffset);

            long lag = Math.Max(0, endOffset - committed);
            totalLag += lag;
            processedTotal += committed;

            // --- Watermark timestamps ---
            DateTimeOffset? producedTs = null;
            DateTimeOffset? processedTs = null;

            if (endOffset > 0)
                producedTs = ReadTimestampAt(monitor, tp, endOffset - 1);      // last produced event

            if (committed > 0)
                processedTs = ReadTimestampAt(monitor, tp, committed - 1);     // last processed event

            long? delayMs = null;
            if (producedTs.HasValue && processedTs.HasValue)
            {
                var d = (long)(producedTs.Value - processedTs.Value).TotalMilliseconds;
                if (d >= 0) delayMs = d;
            }
            if (delayMs.HasValue) delays.Add(delayMs.Value);

            perPartition.Add(new
            {
                partition = tp.Partition.Value,
                committed,
                end = endOffset,
                lag,
                producedTsUnixMs = producedTs.HasValue ? new DateTimeOffset(producedTs.Value.UtcDateTime).ToUnixTimeMilliseconds() : (long?)null,
                processedTsUnixMs = processedTs.HasValue ? new DateTimeOffset(processedTs.Value.UtcDateTime).ToUnixTimeMilliseconds() : (long?)null,
                watermarkDelayMs = delayMs
            });
        }

        long watermarkDelayMsMax = delays.Count > 0 ? delays.Max() : 0;
        long watermarkDelayMsAvg = delays.Count > 0 ? (long)delays.Average() : 0;

        return Results.Json(new
        {
            topic,
            consumerGroup = group,
            totalLag,
            processedTotal,
            endTotal,
            watermarkDelayMsMax,
            watermarkDelayMsAvg,
            perPartition
        });
    }
    catch (Exception ex)
    {
        return Results.Json(new { error = ex.GetType().Name, message = ex.Message }, statusCode: 500);
    }
});

app.Run();
