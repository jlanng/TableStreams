using System.Collections.Immutable;
using System.Reactive;
using Microsoft.Reactive.Testing;

namespace TableStreams.Tests;

public static class TestHelper
{
    /// <summary>
    /// Convenience utility that builds a cold observable from table row changes, with the index being automatically maintained
    /// </summary>
    public static IndexedTableStream<TKey, TValue> CreateColdIndexedTableStream<TKey, TValue>(
        this TestScheduler testScheduler,
        params (long Time, TableRowChange<TKey, TValue>[] Changes)[] changeSets) where TKey : notnull
    {
        var index = ImmutableDictionary<TKey, TValue>.Empty;

        var recordings = new List<Recorded<Notification<IndexedTableStreamUpdate<TKey, TValue>>>>(changeSets.Length);
        foreach (var recordedTableRowChangeSet in changeSets.OrderBy(x=>x.Time))
        {
            var indexBuilder = index.ToBuilder();
            
            foreach (var change in recordedTableRowChangeSet.Changes)
            {
                change.Match(
                    insert => { indexBuilder.Add(insert.Key, insert.InsertedValue); },
                    update => { indexBuilder[update.Key] = update.UpdatedValue;},
                    delete => { indexBuilder.Remove(delete.Key); }
                );
            }

            index = indexBuilder.ToImmutable();
            
            recordings.Add(new Recorded<Notification<IndexedTableStreamUpdate<TKey, TValue>>>(
                recordedTableRowChangeSet.Time,
                Notification.CreateOnNext(new IndexedTableStreamUpdate<TKey, TValue>(index, recordedTableRowChangeSet.Changes))));
        }

        var underlyingObservable = testScheduler.CreateColdObservable(recordings.ToArray());

        return new IndexedTableStream<TKey, TValue>(underlyingObservable);
    }

    /// <summary>
    /// helper to reduce boilerplate
    /// </summary>
    public static IReadOnlyList<Recorded<Notification<IndexedTableStreamUpdate<TKey, TValue>>>> RunScheduledTableStreamFixture<TKey, TValue>(
        Func<TestScheduler, IIndexedTableStream<TKey, TValue>> f) where TKey : notnull
    {
        var testScheduler = new TestScheduler();
        
        var stream = f(testScheduler);

        var recorder = testScheduler.CreateObserver<IndexedTableStreamUpdate<TKey, TValue>>();
        
        using (stream.Subscribe(recorder))
        {
            testScheduler.Start();
        }

        return recorder.Messages.ToArray();
    }
}
