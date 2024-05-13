using System.Collections.Immutable;
using System.Reactive.Subjects;

namespace TableStreams.Testing;

public class IndexedTableStreamSubject<TKey, TValue> : IIndexedTableStream<TKey, TValue>
    where TKey : notnull
{
    private readonly Subject<IndexedTableStreamUpdate<TKey, TValue>> _subject = new();
    private ImmutableDictionary<TKey, TValue> _index = ImmutableDictionary<TKey, TValue>.Empty;

    public IObservable<IndexedTableStreamUpdate<TKey, TValue>> UnderlyingStream => _subject;

    public void OnNext(TableRowChange<TKey, TValue>[] changes)
    {
        var indexBuilder = _index.ToBuilder();
        
        foreach (var change in changes)
        {
            change.Match(
                insert => { indexBuilder.Add(insert.Key, insert.InsertedValue); },
                update => { indexBuilder[update.Key] = update.UpdatedValue;},
                delete => { indexBuilder.Remove(delete.Key); }
                );
        }

        _index = indexBuilder.ToImmutable();
        
        _subject.OnNext(new IndexedTableStreamUpdate<TKey, TValue>(_index, changes));
    }
    
    public void OnNext(TableRowChange<TKey, TValue> change)
    {
        OnNext([change]);
    }
    
    public void OnNextInsert(TKey key, TValue value)
    {
        OnNext([new Insert<TKey, TValue>(key, value)]);
    }
    
    public void OnNextUpdate(TKey key, TValue updatedValue)
    {
        var currentValue = _index[key];
        
        OnNext([new Update<TKey, TValue>(key, currentValue, updatedValue)]);
    }
    
    public void OnNextDelete(TKey key)
    {
        var currentValue = _index[key];
        
        OnNext([new Delete<TKey, TValue>(key, currentValue)]);
    }

    public void OnCompleted()
    {
        _subject.OnCompleted();
    }

    public void OnError(Exception error)
    {
        _subject.OnError(error);
    }
}