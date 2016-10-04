using System;
using Microsoft.WindowsAzure.Storage.Table;

class DedupeEntity : TableEntity, DedupeClaim
{
    public DedupeEntity(string partitionKey)
    {

        PartitionKey = partitionKey;
        RowKey = PartitionKey;
    }

    public string MessageId { get; set; }
    public DedupeEntity() { }
    public bool IsProcessed { get; set; }
    public DateTime LeaseExpiration { get; set; }
}