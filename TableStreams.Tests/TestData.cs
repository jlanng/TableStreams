using System.Reactive.Concurrency;
using System.Reactive.Linq;
using LanguageExt;
using Microsoft.Reactive.Testing;

namespace TableStreams.Tests;


public static class TestData
{
    public static class CompanyStructure
    {
        public readonly record struct Employee(string EmployeeId, string Name,  Option<string> ManagerEmployeeId);

        public static readonly IReadOnlyDictionary<string, Employee> Employees = new Employee[]
            {
                new("E001", "Grande Fromage", Option<string>.None),

                new("E002", "Andy Assistant", "E001"),
                new("E003", "Marcie Manager", "E001"),

                new("E010", "Wanetta Worker", "E003"),
                new("E011", "Wilberforce Worker", "E003"),
            }
            .ToDictionary(x => x.EmployeeId);
    }
    
    public static class Faang
    {
        public static readonly string[] InstrumentUniverse = ["AAPL", "AMZN", "GOOG", "META", "NFLX"];
        
        public enum Sentiment
        {
            Negative,
            Neutral,
            Positive
        };
        public readonly record struct InstrumentNews(string Ticker, Sentiment Sentiment, string News);
        public readonly record struct InstrumentPrice(string Ticker, decimal Price);
        
        public static IObservable<InstrumentNews> BuildNewsStream(IScheduler testScheduler, int randomSeed, TimeSpan tickFrequency)
        {
            var random = new Random(randomSeed);
            
            var sentimentValues = Enum.GetValues<Sentiment>();

            return Observable.Interval(tickFrequency, testScheduler)
                .SelectMany(_ => InstrumentUniverse)
                .Select(ticker =>
                {
                    var news = RandomData.GenerateWords(random);
                    var sentiment = sentimentValues[random.Next(sentimentValues.Length)];
              
                    return new InstrumentNews(ticker, sentiment, news);
                });
        }
        
        public static IObservable<InstrumentPrice> BuildPriceStream(TestScheduler testScheduler, int randomSeed, TimeSpan tickFrequency)
        {
            var random = new Random(randomSeed);

            return Observable.Interval(tickFrequency, testScheduler)
                .SelectMany(_ => InstrumentUniverse)
                .Select(ticker =>
                {
                    var price = RandomData.GeneratePrice(random);
                    
                    return new InstrumentPrice(ticker, price);
                });
        }
    }
}
