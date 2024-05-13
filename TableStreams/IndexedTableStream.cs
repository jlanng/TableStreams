using System.Collections.Immutable;
using System.Reactive.Linq;
using LanguageExt;

namespace TableStreams;

/// <summary>
/// Both the latest index and the most recent set of changes that gave rise to the index
/// </summary>
/// <param name="Index">Index represents the state of the table after Changes have been applied</param>
/// <param name="Changes">Changes since last update which lead to current state in Index</param>
public readonly record struct IndexedTableStreamUpdate<TKey, TValue>(IReadOnlyDictionary<TKey, TValue> Index, TableRowChange<TKey, TValue>[] Changes);

public interface IIndexedTableStream<TKey, TValue>
    where TKey: notnull
{
    IObservable<IndexedTableStreamUpdate<TKey, TValue>> UnderlyingStream { get; }
}

public class IndexedTableStream<TKey, TValue> : IIndexedTableStream<TKey, TValue> where TKey : notnull
{
    public static IndexedTableStream<TKey, TValue> FromObservable(IObservable<TValue> valueStream, Func<TValue, TKey> keyExtractor, Func<TValue, bool> isDeletedExtractor)
    {
        var resultStream = Observable.Create<IndexedTableStreamUpdate<TKey, TValue>>(observer =>
        {
            var state = ImmutableDictionary<TKey, TValue>.Empty;
            
            return valueStream.Subscribe(
                newValue =>
                {
                    var key = keyExtractor(newValue);
                    var isDeleted = isDeletedExtractor(newValue);
                
                    var reduced = ReduceUpdate(state, key, newValue, isDeleted);

                    state = reduced.Index;
                    reduced.UpdateOption.IfSome(update => observer.OnNext(new IndexedTableStreamUpdate<TKey, TValue>(state, [update])));
                },
                observer.OnError,
                observer.OnCompleted);
        });

        return new IndexedTableStream<TKey, TValue>(resultStream);
    }

    readonly record struct ReducerResult(ImmutableDictionary<TKey, TValue> Index, Option<TableRowChange<TKey, TValue>> UpdateOption);

    private static ReducerResult ReduceUpdate(
        ImmutableDictionary<TKey, TValue> state,
        TKey key,
        TValue newValue,
        bool isDeleted)
    {
        var rowAlreadyExistsForKey = state.TryGetValue(key, out var currentValue);

        if (isDeleted)
        {
            if (!rowAlreadyExistsForKey)
            {
                return new ReducerResult(state, Option<TableRowChange<TKey, TValue>>.None);
            }

            return new ReducerResult(
                state.Remove(key),
                new Insert<TKey, TValue>(key, newValue)
            );
        }

        if (!rowAlreadyExistsForKey)
        {
            return new ReducerResult(
                state.SetItem(key, newValue),
                new Insert<TKey, TValue>(key, newValue)
            );
        }

        var isSpuriousUpdate = currentValue!.Equals(newValue);
        if (isSpuriousUpdate)
        {
            return new ReducerResult(state, Option<TableRowChange<TKey, TValue>>.None);
        }
                        
        return new ReducerResult(
            state.SetItem(key, newValue), 
            new Update<TKey, TValue>(key, currentValue, newValue)
        );

    }

    internal IndexedTableStream(IObservable<IndexedTableStreamUpdate<TKey, TValue>> underlyingStream)
    {
        UnderlyingStream = underlyingStream;
    }

    public IObservable<IndexedTableStreamUpdate<TKey, TValue>> UnderlyingStream { get; }
}

