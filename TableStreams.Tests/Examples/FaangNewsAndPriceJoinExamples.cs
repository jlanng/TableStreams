using System.Globalization;
using System.Reactive.Linq;
using System.Text;
using LanguageExt;
using Microsoft.Reactive.Testing;

namespace TableStreams.Tests.Examples;

public class FaangNewsAndPriceJoinExamples
{
    const string MissingValueString = "N/A";

    readonly record struct JoinedInstrument(string Ticker)
    {
        public Option<TestData.Faang.InstrumentPrice> Price { get; init; } = Option<TestData.Faang.InstrumentPrice>.None;
        public Option<TestData.Faang.InstrumentNews> News { get; init; } = Option<TestData.Faang.InstrumentNews>.None;

        public override string ToString()
        {
            return $"{Ticker}: LastPx={Price.Match(instrumentPrice => instrumentPrice.Price.ToString(CultureInfo.InvariantCulture), () => MissingValueString)}, LastNews={News.Match(news => $"{news.News} ({news.Sentiment})", () => MissingValueString)} ";
        }
    }
    
    [Fact]
    public void Faang_LeftJoinsWhenRightIsInsertedOrUpdated_ReturnsJoinedResults()
    {
        const int randomSeed = 42;
        var testScheduler = new TestScheduler();

        var instrumentsTableStream = TestData.Faang.InstrumentUniverse.ToDictionary(ticker => ticker).AsIndexedTableStream();
        var hourlyNewsTableStream = TestData.Faang.BuildNewsStream(testScheduler, randomSeed, TimeSpan.FromHours(1)).AsIndexedTableStream(x => x.Ticker);
        var twiceHourlyPriceTableSteam = TestData.Faang.BuildPriceStream(testScheduler, randomSeed, TimeSpan.FromHours(0.5)).AsIndexedTableStream(x => x.Ticker);

        var joinedStream = instrumentsTableStream
            .LeftJoin(twiceHourlyPriceTableSteam, ticker => ticker, (ticker, price) => new JoinedInstrument(ticker) with { Price = price })
            .LeftJoin(hourlyNewsTableStream, joinedInstrument => joinedInstrument.Ticker, (joinedInstrument, news) => joinedInstrument with { News = news });

        var instrumentHistoryBuilder = new StringBuilder();
        using (joinedStream.UnderlyingStream
                   .SelectMany(update => update.Changes)
                   .Subscribe(change =>
                   {
                       instrumentHistoryBuilder.AppendLine(
                           change.Match(
                               insert => $"INS {insert.InsertedValue}",
                               update => $"UPD {update.UpdatedValue}",
                               delete => $"DEL {delete.Key}" )
                       );
                   }))
        {
            testScheduler.AdvanceBy(TimeSpan.FromHours(1).Ticks+1);
        }

        const string expected = @"INS AAPL: LastPx=N/A, LastNews=N/A 
INS AMZN: LastPx=N/A, LastNews=N/A 
INS GOOG: LastPx=N/A, LastNews=N/A 
INS META: LastPx=N/A, LastNews=N/A 
INS NFLX: LastPx=N/A, LastNews=N/A 
UPD AAPL: LastPx=66.81, LastNews=N/A 
UPD AMZN: LastPx=14.09, LastNews=N/A 
UPD GOOG: LastPx=12.55, LastNews=N/A 
UPD META: LastPx=52.27, LastNews=N/A 
UPD NFLX: LastPx=16.84, LastNews=N/A 
UPD AAPL: LastPx=66.81, LastNews=di hi fogene hibop ci nasaxinab vanosed o datequa cesiga hasam (Positive) 
UPD AMZN: LastPx=14.09, LastNews=mavaba boxekibe lo lexisa yesadejoda piy seporajo a bap (Negative) 
UPD GOOG: LastPx=12.55, LastNews=de toba waxohiquif bax dofepisap leyenovada xil doyof quovih vawife vic woyomoyap nidojis tifoca (Negative) 
UPD META: LastPx=52.27, LastNews=he memorokoja pikocewexow pawe rohotaji i kegiqu rado (Negative) 
UPD NFLX: LastPx=16.84, LastNews=bika qua firokile tin roraj repomonob filay mila tex gowiqu jadoqu fededar xov dohicar (Neutral) 
UPD AAPL: LastPx=26.25, LastNews=di hi fogene hibop ci nasaxinab vanosed o datequa cesiga hasam (Positive) 
UPD AMZN: LastPx=72.44, LastNews=mavaba boxekibe lo lexisa yesadejoda piy seporajo a bap (Negative) 
UPD GOOG: LastPx=51.29, LastNews=de toba waxohiquif bax dofepisap leyenovada xil doyof quovih vawife vic woyomoyap nidojis tifoca (Negative) 
UPD META: LastPx=17.36, LastNews=he memorokoja pikocewexow pawe rohotaji i kegiqu rado (Negative) 
UPD NFLX: LastPx=76.12, LastNews=bika qua firokile tin roraj repomonob filay mila tex gowiqu jadoqu fededar xov dohicar (Neutral) 
";
        
        Assert.Equal(expected, instrumentHistoryBuilder.ToString());
    }
}