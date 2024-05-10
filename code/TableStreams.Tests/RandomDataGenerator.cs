using System.Text;

namespace TableStreams.Tests;

public static class RandomData
{
    public static string GenerateWords(Random random)
    {
        var wordCount = random.Next(5, 15);
        
        return string.Join(
            " ", 
            Enumerable.Range(0, wordCount).Select(_ =>
            {
                var wordLength = random.Next(1, 12);
                
                return CreateWord(random, wordLength);
            })); 
    }
    
    public static string CreateWord(Random random, int requestedLength)
    {
        string[] consonants = { "b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p", "q", "r", "s", "t", "v", "w", "x", "y", "z" };
        string[] vowels = { "a", "e", "i", "o", "u" };

        string word = "";

        if (requestedLength == 1)
        {
            word = GetRandomLetter(random, vowels);
        }
        else
        {
            for (int i = 0; i < requestedLength; i+=2)
            {
                word += GetRandomLetter(random, consonants) + GetRandomLetter(random, vowels);
            }

            word = word.Replace("q", "qu").Substring(0, requestedLength); // We may generate a string longer than requested length, but it doesn't matter if cut off the excess.
        }

        return word;

        static string GetRandomLetter(Random rnd, string[] letters)
        {
            return letters[rnd.Next(0, letters.Length - 1)];
        }
    }
    
    public static decimal GeneratePrice(Random random)
    {
        return decimal.Divide(random.Next(10000), 100);
    }
}