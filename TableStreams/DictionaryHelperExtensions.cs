using LanguageExt;

namespace TableStreams;

internal static class DictionaryHelperExtensions
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
    
    public static bool DeepEquals<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> left, IReadOnlyDictionary<TKey, TValue> right)
    {
        if (!left.Count.Equals(right.Count)) return false;

        if (!left.Keys.SequenceEqual(right.Keys)) return false;

        return left.Keys.All(key =>
        {
            var leftValue = left[key];
            var rightValue = right[key];

            if (leftValue == null)
            {
                return rightValue == null;
            }

            return leftValue.Equals(rightValue);
        });

    }
}