using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System.Text.Json;

public class KafkaProducer<TShema>
{
    public readonly ProducerConfig _config;
    public readonly string _topic;
    public readonly IProducer<Null, TShema> producer;
    public KafkaProducer(ProducerConfig config,string topic)
    {
        this._config = config;
        this._topic = topic;
        var schemaRegestry = new CachedSchemaRegistryClient(
                new SchemaRegistryConfig
                {
                    Url = "http://schema-registry:8081"
                });
        this.producer = new ProducerBuilder<Null, TShema>(_config)
            .SetValueSerializer(new AvroSerializer<TShema>(schemaRegestry))
            .Build();
    }
    public async Task SendMessageAsync(TShema payload)
    {

        

        await this.producer.ProduceAsync(
            _topic,
            new Message<Null, TShema>
            {
                Value = payload
            });
    }
}