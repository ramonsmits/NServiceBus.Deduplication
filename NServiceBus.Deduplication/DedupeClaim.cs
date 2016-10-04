using System;

public interface DedupeClaim
{
    bool IsProcessed { get; }
    DateTime LeaseExpiration { get; }
}