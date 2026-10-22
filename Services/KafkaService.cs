using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Mapper;

namespace kafkaTestApp.Services;

public class KafkaService
{
    public KafkaProducer<OrderAv> _producer;
    public OrderToOrderAv _mapper;
    public KafkaService(KafkaProducer<OrderAv> producer, OrderToOrderAv mapper)
    {
        this._producer = producer;
        this._mapper = mapper;
    }

    public async Task SendMessageAsync(OrderTest order)
    {
        var orderAv = this._mapper.toOrderAv(order);

        await this._producer.SendMessageAsync(orderAv);
    }


}
