using Avro;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using kafkaTestApp.Kafka.dlqType;

namespace kafkaTestApp.Kafka.Consumer;
public abstract class KafkaConsumerBase<TPayload>: BackgroundService
{
    private readonly KafkaProducer<DlqType<TPayload>> _kafkaDlqProducer;
    public ConsumerConfig? _config { get; set; } = default;
    public string _topic { get; set; } = string.Empty;
    public KafkaConsumerBase(KafkaProducer<DlqType<TPayload>> kafkaDlqProducer) {
        _kafkaDlqProducer = kafkaDlqProducer;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(()=> ConsumeMessages(stoppingToken),stoppingToken);
    }

    public async Task ConsumeMessages(CancellationToken cancellationToken) {

        var schemaRegestry = new CachedSchemaRegistryClient(
         new SchemaRegistryConfig
         {
             Url = "http://localhost:8081"
         });

        using var consumer = new ConsumerBuilder<Ignore, TPayload>(_config)
            .SetValueDeserializer(new AvroDeserializer<TPayload>(schemaRegestry).AsSyncOverAsync())
            .Build();

        consumer.Subscribe(_topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(cancellationToken);
                try
                {
                    Console.WriteLine($"Message received: {result.Message.Value}");

                    await ProcessMessageWithRetries(result.Message.Value);

                    consumer.Commit(result);
                }
                catch (Exception ex) {
                    Console.WriteLine($"Message failed after retries: {ex.Message}");
                    try
                    {
                        var dlqMessage = new DlqType<TPayload>
                        {
                            OriginalMessage = result.Message.Value,
                            Error = ex.Message,
                            FailedAt = DateTime.UtcNow
                        };
                        await this._kafkaDlqProducer.SendMessageAsync(dlqMessage);
                        consumer.Commit(result);
                        Console.WriteLine("Messages sent to DLQ and offset commited");
                    }
                    catch(Exception dlqException) {
                        Console.WriteLine($"Failed to send to DLQ: {dlqException.Message}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consumer stopped !");
        }
        finally
        {
            consumer.Close();
        }

    }

    private async Task ProcessMessageWithRetries(TPayload message)
    {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try
            {
                await ProcessMesage(message);

                Console.WriteLine("Message traité (done)");
                return;
            }
            catch (Exception exp)
            {
                Console.WriteLine("Retry number "+ (i+1) + " " + exp.Message);
                if (i == maxRetries-1) throw;
                int delay = 1000 * (int)Math.Pow(2, i);
                await Task.Delay(delay);
            }
        }
    }
    public abstract Task ProcessMesage(TPayload message);

}