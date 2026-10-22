using Confluent.Kafka;
using Confluent.SchemaRegistry;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.dlqType;

namespace kafkaTestApp.Kafka.Consumer;

public class KafkaOrderConsumer : KafkaConsumerBase<OrderAv>
{
    public string topic = "order-topic";
    public KafkaOrderConsumer(KafkaProducer<DlqType<OrderAv>> kafkaDlqProducer) : base(kafkaDlqProducer)
    {
        this._topic = topic;
        this._config = new ConsumerConfig
        {
            BootstrapServers = "broker:9092",
            GroupId = "orders-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

    }
    public override async Task ProcessMesage(OrderAv message)
    {
        Console.WriteLine($"Processing: {message.Id} - { message.Price} - { message.Price}");
        await Task.Delay(1000);
        //throw new Exception("Erreur de traitement simulée");
    }
}

