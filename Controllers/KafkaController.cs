using Confluent.Kafka;
using kafkaTestApp.Services;
using Microsoft.AspNetCore.Mvc;

namespace kafkaTestApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KafkaController : ControllerBase
    {
        public KafkaService _kafkaService;

        public KafkaController(KafkaService kafkaService) {
            this._kafkaService = kafkaService;
        }

        [HttpPost]
        public async Task<ActionResult<OrderTest>> addMessage(OrderTest order)
        {
            try
            {
                await this._kafkaService.SendMessageAsync(order);

                return Ok(order);

            }catch(Exception exp)
            {
                return StatusCode(500, exp.Message);
            }
            
        }
    }
}
