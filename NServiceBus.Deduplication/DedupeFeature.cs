using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Logging;

class DedupeFeature : Feature
{
    static readonly ILog Log = LogManager.GetLogger(nameof(DedupeFeature));
    public DedupeFeature()
    {
        EnableByDefault();
    }

    protected override void Setup(FeatureConfigurationContext context)
    {
        var providerTypeName = "AzureStorageDedupeStorageProvider, NServiceBus.Deduplication.AzureTableStorage";
        //var providerTypeName = "InMemoryDedupeStorageProvider, NServiceBus.Deduplication";
        var providerType = Type.GetType(providerTypeName);
        var provider = (DedupeStorageProvider)Activator.CreateInstance(providerType);

        Log.InfoFormat("Provider type: {0}", provider);

        var instance = new DeduplicationBehavior(provider);
        context.Container.RegisterSingleton(instance);
        context.Pipeline.Register(nameof(DeduplicationBehavior), typeof(DeduplicationBehavior), "Dedupe messages.");
        context.RegisterStartupTask(new CleanupTask(provider.Cleanup));
    }

    class CleanupTask : FeatureStartupTask
    {
        readonly TimeSpan Interval = TimeSpan.FromSeconds(60);
        readonly ILog Log = LogManager.GetLogger(nameof(CleanupTask));
        readonly IntervalScheduler Scheduler = new IntervalScheduler(TimeSpan.FromSeconds(10));

        CancellationTokenSource cancellationTokenSource;
        CancellationToken cancellationToken;
        Task loopTask;
        readonly Func<CancellationToken, Task> Cleanup;

        public CleanupTask(Func<CancellationToken, Task> task)
        {
            Cleanup = task;
        }

        protected override Task OnStart(IMessageSession session)
        {
            cancellationTokenSource = new CancellationTokenSource();
            cancellationToken = cancellationTokenSource.Token;

            loopTask = Task.Run(Loop);
            return Task.FromResult(0);
        }

        protected override Task OnStop(IMessageSession session)
        {
            cancellationTokenSource.Cancel();
            return loopTask;
        }

        async Task Loop()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var start = DateTime.UtcNow;
                    await Cleanup(cancellationToken);

                    var now = DateTime.UtcNow;
                    var duration = now - start;

                    if (duration > Interval)
                    {
                        Log.WarnFormat("Took more time ({0}) than the interval ({1}). Skipping delay.", duration, Interval);
                        continue;
                    }

                    var next = Scheduler.Next(start);
                    var delay = next - now;
                    Log.DebugFormat("Delaying {0}", delay);
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Log.Error("Cleanup", ex);
                }
            }
        }
    }
}