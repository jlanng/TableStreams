using System.Reactive.Linq;

namespace TableStreams;

public static class AsIndexedTableStreamExtensions
{
    public static IIndexedTableStream<TKey, TValue> AsIndexedTableStream<TKey, TValue>(this IObservable<TValue> source, Func<TValue, TKey> keyExtractor)
        where TKey : notnull
    {
        return IndexedTableStream<TKey, TValue>.FromObservable(source, keyExtractor, _ => false);
    }
    
    public static IIndexedTableStream<TKey, TValue> AsIndexedTableStream<TKey, TValue>(this IObservable<TValue> source, Func<TValue, TKey> keyExtractor, Func<TValue, bool> isDeletedExtractor)
        where TKey : notnull
    {
        return IndexedTableStream<TKey, TValue>.FromObservable(source, keyExtractor, isDeletedExtractor);
    }
    
    public static IIndexedTableStream<TKey, TValue> AsIndexedTableStream<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> source)
        where TKey : notnull
    {
        var snapshot = new IndexedTableStreamUpdate<TKey, TValue>(
            source,
            source.Select(entry => new Insert<TKey, TValue>(entry.Key, entry.Value)).Cast<TableRowChange<TKey, TValue>>().ToArray()
        );

        return new IndexedTableStream<TKey, TValue>(Observable.Return(snapshot));
    }
}