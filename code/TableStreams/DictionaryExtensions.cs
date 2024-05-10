using LanguageExt;

namespace TableStreams;

public static class DictionaryExtensions
{
    public static Option<TValue> GetValueOption<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> source, TKey key)
    {
        return source.TryGetValue(key, out var value) ? Option<TValue>.Some(value) : Option<TValue>.None;
    }
    
    public static TValue GetOrAdd<TKey, TValue>(this IDictionary<TKey, TValue> source, TKey key, Func<TValue> factory)
    {
        if (source.TryGetValue(key, out var value))
        {
            return value;
        }
        
        var newDict = factory();
        source.Add(key, newDict);

        return newDict;
    }
}