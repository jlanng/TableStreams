namespace TableStreams;

/// <summary>
/// Discriminated union of all possible change types
///
/// Includes previous value in the case of updates and deletes since downstream operators will find this useful
/// (e.g. mean average)
/// </summary>
public abstract record TableRowChange<TKey, TValue>
{
    public TMatchResult Match<TMatchResult>(
        Func<Insert<TKey, TValue>, TMatchResult> insert, 
        Func<Update<TKey, TValue>, TMatchResult> update,
        Func<Delete<TKey, TValue>, TMatchResult> delete) => this switch
        {
            Insert<TKey, TValue> j  => insert(j),
            Update<TKey, TValue> j  => update(j),
            Delete<TKey, TValue> j  => delete(j),
            _ => throw new NotSupportedException()
        };
    
    public void Match(
        Action<Insert<TKey, TValue>> insert, 
        Action<Update<TKey, TValue>> update,
        Action<Delete<TKey, TValue>> delete)
    {
        switch (this)
        {
            case Insert<TKey, TValue> j:
                insert(j);
                break;
            case Update<TKey, TValue> j:
                update(j);
                break;
            case Delete<TKey, TValue> j:
                delete(j);
                break;
            default:
                throw new NotSupportedException();
        }
    }
}
public record Insert<TKey, TValue>(TKey Key, TValue InsertedValue) : TableRowChange<TKey, TValue>;
public record Update<TKey, TValue>(TKey Key, TValue PreviousValue, TValue UpdatedValue) : TableRowChange<TKey, TValue>;
public record Delete<TKey, TValue>(TKey Key, TValue DeletedValue) : TableRowChange<TKey, TValue>;
