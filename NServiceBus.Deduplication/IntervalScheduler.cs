using System;

class IntervalScheduler
{
    readonly TimeSpan Interval;

    public IntervalScheduler(TimeSpan interval)
    {
        Interval = interval;
    }

    public DateTime Next(DateTime last)
    {
        var now = DateTime.UtcNow;
        var next = last;
        do
        {
            next += Interval;
        } while (next < now);
        return next;
    }
}