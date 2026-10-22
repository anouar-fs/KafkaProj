using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;

namespace kafkaTestApp.Mapper;

public class OrderToOrderAv
{
    public OrderAv toOrderAv(OrderTest order)
    {
        return new OrderAv
        {
            Id = order.Id,
            Product = order.Product,
            Price = order.Price,
            Quantity = order.Quantity
        };
    }
}

