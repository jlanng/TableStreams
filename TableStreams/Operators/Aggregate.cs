using System.Reactive.Linq;

namespace TableStreams.Operators;

internal static class AggregateOperator
{
    public static async Task<IReadOnlyDictionary<TKey, TValue>?> Aggregate<TKey, TValue>(IIndexedTableStream<TKey, TValue> source) where TKey : notnull
    {
        return await source.UnderlyingStream
            .Aggregate(default(IReadOnlyDictionary<TKey, TValue>), (_, next) => next.Index);
    }
    
}