using System.Reactive.Linq;
using LanguageExt;
using TableStreams.Operators;

namespace TableStreams;

/// <summary>
/// Ergonomic access to operators
/// </summary>
public static class IndexedTableStreamExtensions
{
    public static IIndexedTableStream<TLeftKey, TResult>
        LeftJoin<TLeftKey, TLeftValue, TRightKey, TRightValue, TResult>(
            this IIndexedTableStream<TLeftKey, TLeftValue> leftSource,
            IIndexedTableStream<TRightKey, TRightValue> rightSource,
            Func<TLeftValue, Option<TRightKey>> foreignKeyExtractor,
            Func<TLeftValue, Option<TRightValue>, TResult> resultSelector) where TLeftKey : notnull where TRightKey : notnull
    {
        return LeftJoinOperator.LeftJoin(leftSource, rightSource, foreignKeyExtractor, resultSelector);
    }

    public static Task<IReadOnlyDictionary<TKey, TValue>?> Aggregate<TKey, TValue>(this IIndexedTableStream<TKey, TValue> source)
        where TKey : notnull
    {
        return AggregateOperator.Aggregate(source);
    }
    
    public static IIndexedTableStream<TResultKey, TResultValue> Publish<TSourceKey, TSourceValue, TResultKey, TResultValue>(
        this IIndexedTableStream<TSourceKey, TSourceValue> source,
        Func<IIndexedTableStream<TSourceKey, TSourceValue>, IIndexedTableStream<TResultKey, TResultValue>> selector)
        where TSourceKey : notnull where TResultKey : notnull
    {
        var underlying = source.UnderlyingStream;

        var selected =
            underlying
                .Publish<IndexedTableStreamUpdate<TSourceKey, TSourceValue>, IndexedTableStreamUpdate<TResultKey, TResultValue>>(
                    publishedUnderlying =>
                    {
                        var wrapped = new IndexedTableStream<TSourceKey, TSourceValue>(publishedUnderlying);

                        var result = selector(wrapped);

                        return result.UnderlyingStream;
                    });

        return new IndexedTableStream<TResultKey, TResultValue>(selected);
    }
    
    public static IIndexedTableStream<TKey, TValue> ConcatWith<TKey, TValue>(
        this IIndexedTableStream<TKey, TValue> source,
        IndexedTableStreamUpdate<TKey, TValue> second)
        where TKey : notnull
    {
        var concatenated = source.UnderlyingStream
            .Concat(Observable.Return(second));

        return new IndexedTableStream<TKey, TValue>(concatenated);
    }

    public static IDisposable Subscribe<TKey, TValue>(
        this IIndexedTableStream<TKey, TValue> source,
        IObserver<IndexedTableStreamUpdate<TKey, TValue>> observer) where TKey : notnull
    {
        return source.UnderlyingStream.Subscribe(observer);
    }
}