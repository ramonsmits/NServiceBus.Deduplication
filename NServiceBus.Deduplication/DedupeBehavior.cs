using System;
using System.Threading.Tasks;
using NServiceBus.Logging;
using NServiceBus.Pipeline;

class DeduplicationBehavior : Behavior<IIncomingPhysicalMessageContext>//ITransportReceiveContext
                                                                       //IIncomingPhysicalMessageContext
{
    readonly ILog Log = LogManager.GetLogger(nameof(DeduplicationBehavior));
    readonly DedupeStorageProvider Storage;

    public DeduplicationBehavior(DedupeStorageProvider storage)
    {
        Storage = storage;
    }

    public override async Task Invoke(IIncomingPhysicalMessageContext context, Func<Task> next)
    {
        var id = context.MessageId;//.MessageId;

        var claim = await Storage.GetLease(id);

        if (claim.IsProcessed)
        {
            Log.InfoFormat("Skip processing of message {0} as it is already processed.", id);
            return;
        }

        try
        {
            await next();
            var now = DateTime.UtcNow;
            if (now > claim.LeaseExpiration) Log.WarnFormat("Dedupe lease already expired ({0:N}s) for message {1}!", now - claim.LeaseExpiration, id);
            Log.DebugFormat("Completing dedupe lease for message {0}.", id);
            await Storage.CompleteLease(claim);
        }
        catch
        {
            Log.DebugFormat("Releasing dedupe lease for message {0}.", id);
            await Storage.ReleaseLease(claim);
            throw;
        }
    }
}