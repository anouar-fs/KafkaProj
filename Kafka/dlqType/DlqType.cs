namespace kafkaTestApp.Kafka.dlqType;
public class DlqType<Tpayload>
{
    public required Tpayload OriginalMessage { get; set; }
    public string Error { get; set; } = string.Empty;
    public DateTime FailedAt { get; set; }
}

