using System.Collections.Immutable;
using System.Reactive.Linq;
using System.Text.RegularExpressions;

namespace TableStreams.Operators;

internal static class AggregateOperator
{
    public static IndexedTableStream<TKey, TValue> Aggregate<TKey, TValue>(IIndexedTableStream<TKey, TValue> source) where TKey : notnull
    {
        var stream = source.UnderlyingStream
            .Aggregate(default(IReadOnlyDictionary<TKey,TValue>), (_, next) => next.Index)
            .Select(dict =>
            {
                return new IndexedTableStreamUpdate<TKey, TValue>(
                    dict!,
                    dict!.Select(entry => new Insert<TKey, TValue>(entry.Key, entry.Value)).Cast<TableRowChange<TKey, TValue>>().ToArray()
                    );
            });

        return new IndexedTableStream<TKey, TValue>(stream);
    }
    
}