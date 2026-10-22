
using Confluent.Kafka;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.Consumer;
using kafkaTestApp.Kafka.dlqType;
using kafkaTestApp.Mapper;
using kafkaTestApp.Services;

var Produceconfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    EnableIdempotence = true,
    Acks = Acks.All,
    LingerMs = 50
};


var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<KafkaProducer<OrderAv>>( _ => new KafkaProducer<OrderAv>(Produceconfig, "order-topic") );
builder.Services.AddSingleton<KafkaProducer<DlqType<OrderAv>>>( _ => new KafkaProducer<DlqType<OrderAv>>(Produceconfig, "orders-dlq") );
builder.Services.AddTransient<OrderToOrderAv>();
builder.Services.AddScoped<KafkaService>();

builder.Services.AddHostedService<KafkaOrderConsumer>();

var app = builder.Build();
// Add swagger
app.UseSwagger();

app.UseSwaggerUI();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

