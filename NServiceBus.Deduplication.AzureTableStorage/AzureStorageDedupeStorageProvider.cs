using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using NServiceBus.Logging;

class AzureStorageDedupeStorageProvider : DedupeStorageProvider
{
    readonly TimeSpan PurgeDuration = TimeSpan.FromSeconds(30);
    const string TableNameFormat = "dedupe";//"_{0}";
    static readonly ILog Log = LogManager.GetLogger(nameof(AzureStorageDedupeStorageProvider));
    static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    readonly CloudTable Table;

    public AzureStorageDedupeStorageProvider()
    {
        var account = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["AzureStorage"].ConnectionString);
        var client = account.CreateCloudTableClient();
        Table = client.GetTableReference("dedupe");
        Table.CreateIfNotExists();
    }

    public async Task CompleteLease(DedupeClaim instance)
    {
        var entity = (DedupeEntity)instance;

        entity.IsProcessed = true;

        var update = TableOperation.Replace(entity);
        try
        {
            await Table.ExecuteAsync(update); // Don't need to do anything with the result, message processed again
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusMessage == TableErrorCodeStrings.UpdateConditionNotSatisfied)
            {
                throw new Exception("Lease taken by other incoming message. Possible cause is processing duration exceeds lease duration. Possibly more than once execution of message id.");
            }
        }
    }

    public async Task ReleaseLease(DedupeClaim instance)
    {
        var entity = (DedupeEntity)instance;
        entity.LeaseExpiration = DateTime.MinValue;

        var update = TableOperation.Replace(entity);
        try
        {
            await Table.ExecuteAsync(update); // Don't need to do anything with the result, message processed again
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.ExtendedErrorInformation.ErrorCode == TableErrorCodeStrings.UpdateConditionNotSatisfied)
            {
                throw new Exception($"Lease for {entity.PartitionKey} taken by other incoming message, possible more than once execution of message.");
            }
        }
    }

    public async Task<DedupeClaim> GetLease(string messageId)
    {
        var now = DateTime.UtcNow;
        var key = Encode(messageId);
        var entity = new DedupeEntity(key) { MessageId = messageId, LeaseExpiration = DateTime.UtcNow + LeaseDuration };
        var insert = TableOperation.Insert(entity);

        try
        {
            await Table.ExecuteAsync(insert);
            return entity;
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.ExtendedErrorInformation.ErrorCode != TableErrorCodeStrings.EntityAlreadyExists) throw;
        }

        var retrieve = TableOperation.Retrieve<DedupeEntity>(key, key);
        var result = await Table.ExecuteAsync(retrieve);
        entity = (DedupeEntity)result.Result;

        var isLeaseExpired = entity.LeaseExpiration < now;

        if (!isLeaseExpired) throw new Exception($"Lease not expired for {messageId}.");

        entity.LeaseExpiration = now + LeaseDuration;
        var update = TableOperation.Replace(entity);
        try
        {
            await Table.ExecuteAsync(update); // Don't need to do anything with the result, message processed again
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.ExtendedErrorInformation.ErrorCode == TableErrorCodeStrings.UpdateConditionNotSatisfied)
            {
                throw new Exception($"Lease for {messageId} taken by other incoming message.");
            }
        }
        return entity;
    }

    public async Task Cleanup(CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;
        Log.DebugFormat("Going to purge expired rows");
        var purgeTimestamp = start - PurgeDuration;

        var query = new TableQuery();

        query.FilterString = $"Timestamp le datetime'{purgeTimestamp:s}' and IsProcessed eq true";

        var total = 0;
        TableContinuationToken token = null;
        double duration;
        do
        {
            var result = await Table.ExecuteQuerySegmentedAsync(query, token, cancellationToken);
            token = result.ContinuationToken;

            var tasks = new List<Task>(result.Results.Count);

            foreach (var item in result.Results)
            {
                tasks.Add(Table.ExecuteAsync(TableOperation.Delete(item), cancellationToken));
            }

            duration = (DateTime.UtcNow - start).TotalSeconds;
            Log.DebugFormat("Purging {0:N0} entities took {1:N}s (~{2:N}/s).", result.Results.Count, duration, total / duration);
            total += tasks.Count;
        } while (token != null);

        duration = (DateTime.UtcNow - start).TotalSeconds;
        Log.DebugFormat("Purging completed, purged {0:N0} entities in {1:N}s (~{2:N}/s).", total, duration, total / duration);
    }

    static string Encode(string url)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(url);
        var base64 = System.Convert.ToBase64String(keyBytes);
        return base64.Replace('/', '_');
    }

    static String Decode(String encodedKey)
    {
        var base64 = encodedKey.Replace('_', '/');
        byte[] bytes = System.Convert.FromBase64String(base64);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }

}