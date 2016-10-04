using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class InMemoryDedupeStorageProvider : DedupeStorageProvider
{
    static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    static readonly TimeSpan DedupeWindowDuration = TimeSpan.FromSeconds(60 * 10);
    readonly ConcurrentDictionary<string, DedupeClaimImp> Items = new ConcurrentDictionary<string, DedupeClaimImp>();

    public Task<DedupeClaim> GetLease(string messageId)
    {
        var now = DateTime.UtcNow;
        var entity = new DedupeClaimImp
        {
            MessageId = messageId,
            LeaseExpiration = now.Add(LeaseDuration),
            Revision = 1,
            Timestamp = now
        };

        if (Items.TryAdd(messageId, entity)) return Task.FromResult<DedupeClaim>(entity);

        var previous = entity = Items[messageId];

        if (entity.IsProcessed) return Task.FromResult<DedupeClaim>(entity);

        var isLeaseExpired = entity.LeaseExpiration < now;

        if (!isLeaseExpired) throw new Exception($"Lease not expired for {messageId}.");

        entity.LeaseExpiration = now + LeaseDuration;

        if (!Items.TryUpdate(messageId, entity, previous)) throw new Exception($"Lease for {messageId} taken by other incoming message.");

        return Task.FromResult<DedupeClaim>(entity);
    }

    public Task CompleteLease(DedupeClaim instance)
    {
        var previous = (DedupeClaimImp)instance;
        var claim = (DedupeClaimImp)instance;
        claim.IsProcessed = true;
        claim.Revision++;
        claim.Timestamp = DateTime.UtcNow;
        if (!Items.TryUpdate(claim.MessageId, claim, previous)) throw new Exception("Lease taken by other incoming message. Possible cause is processing duration exceeds lease duration. Possibly more than once execution of message id.");
        return Task.FromResult(0);
    }

    public Task ReleaseLease(DedupeClaim instance)
    {
        var previous = (DedupeClaimImp)instance;
        var claim = previous;
        claim.Revision++;
        claim.LeaseExpiration = DateTime.MinValue;
        if (!Items.TryUpdate(claim.MessageId, claim, previous)) throw new Exception($"Lease for {claim.MessageId} taken by other incoming message, possible more than once execution of message.");

        return Task.FromResult(0);
    }

    public Task Cleanup(CancellationToken cancellationToken)
    {
        var expiredThresshold = DateTime.UtcNow - DedupeWindowDuration;
        var expiredKeys = Items.Where(x => x.Value.Timestamp < expiredThresshold).Select(x => x.Key);
        DedupeClaimImp value;
        foreach (var key in expiredKeys) Items.TryRemove(key, out value);
        return Task.FromResult(0);
    }

    struct DedupeClaimImp : DedupeClaim
    {
        public bool Equals(DedupeClaimImp other)
        {
            return string.Equals(MessageId, other.MessageId) && Revision == other.Revision;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is DedupeClaimImp && Equals((DedupeClaimImp)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((MessageId?.GetHashCode() ?? 0) * 397) ^ Revision;
            }
        }

        public string MessageId { get; set; }
        public bool IsProcessed { get; set; }
        public DateTime LeaseExpiration { get; set; }
        public int Revision { get; set; }
        public DateTime Timestamp { get; set; }

        public static bool operator ==(DedupeClaimImp c1, DedupeClaimImp c2)
        {
            return c1.Revision.Equals(c2.Revision);
        }

        public static bool operator !=(DedupeClaimImp c1, DedupeClaimImp c2)
        {
            return !c1.Revision.Equals(c2.Revision);
        }
    }
}