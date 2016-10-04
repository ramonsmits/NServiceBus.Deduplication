using System.Threading;
using System.Threading.Tasks;

public interface DedupeStorageProvider
{
    Task<DedupeClaim> GetLease(string id);
    Task CompleteLease(DedupeClaim instance);
    Task ReleaseLease(DedupeClaim instance);
    Task Cleanup(CancellationToken cancellationToken);
}