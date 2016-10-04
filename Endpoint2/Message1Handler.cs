using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Logging;

public class Message1Handler :
    IHandleMessages<Message1>
{
    static ILog log = LogManager.GetLogger<Message1Handler>();

    public async Task Handle(Message1 message, IMessageHandlerContext context)
    {
        log.Info($"Received Message1: {message.Property}");
        var message2 = new Message2
        {
            Property = "Hello from Endpoint2"
        };

        await Task.Delay(1000).ConfigureAwait(false);
        await context.Reply(message2);
    }
}