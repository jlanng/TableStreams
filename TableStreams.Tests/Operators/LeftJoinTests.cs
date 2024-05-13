using LanguageExt;

namespace TableStreams.Tests.Operators;

public class LeftJoinTests
{
    readonly record struct LeftRecord(Option<int> ForeignKey, string Value);
    readonly record struct RightRecord(string Value);
    readonly record struct JoinedRecord
    {
        public JoinedRecord(string LeftValue, Option<string> RightValue)
        {
            this.LeftValue = LeftValue;
            this.RightValue = RightValue;
        }
        
        public JoinedRecord(LeftRecord leftRecord, Option<RightRecord> rightRecord)
        {
            LeftValue = leftRecord.Value;
            RightValue = rightRecord.Select(x=>x.Value);
        }

        public string LeftValue { get; init; }
        public Option<string> RightValue { get; init; }
    }
    
    [Fact]
    public void LeftRecordWithNoForeignKeyInserted_EmitsLeftSideUpdate()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            var leftTableStream = testScheduler.CreateColdIndexedTableStream(
                        (0, [new Insert<int, LeftRecord>(1, new LeftRecord(Option<int>.None, "LeftValue"))]));
            var rightTableStream = testScheduler.CreateColdIndexedTableStream<int, RightRecord>();
        
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        var expected = new Insert<int, JoinedRecord>(1, new JoinedRecord("LeftValue", Option<string>.None));
        
        Assert.Equal(expected, messages.Single().Value.Value.Changes.Single());
    }
    
    [Fact]
    public void LeftRecordWithUnmatchedForeignKeyInserted_EmitsLeftSideUpdate()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            var leftTableStream = testScheduler.CreateColdIndexedTableStream(
                (0, [new Insert<int, LeftRecord>(1, new LeftRecord(Option<int>.Some(int.MaxValue), "LeftValue"))])
            );
            var rightTableStream = testScheduler.CreateColdIndexedTableStream<int, RightRecord>();
        
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        var expected = new Insert<int, JoinedRecord>(1, new JoinedRecord("LeftValue", Option<string>.None));
        
        Assert.Equal(expected, messages.Single().Value.Value.Changes.Single());
    }
    
    [Fact]
    public void OnlyRightRecordInserted_EmitsNothing()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            var leftTableStream = testScheduler.CreateColdIndexedTableStream<int, LeftRecord>();
            var rightTableStream = testScheduler.CreateColdIndexedTableStream(
                (0, [new Insert<int, RightRecord>(1, new RightRecord("RightValue"))]));
        
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        Assert.Equal(0, messages.Count);
    }
    
    [Fact]
    public void RightRecordInsertedBeforeMatchingLeftRow_EmitsSingleJoinedRowOnly()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            var rightTableStream = testScheduler.CreateColdIndexedTableStream(
                (0, [new Insert<int, RightRecord>(1, new RightRecord("RightValue"))]));
            
            var leftTableStream = testScheduler.CreateColdIndexedTableStream(
                (10, [new Insert<int, LeftRecord>(1, new LeftRecord(Option<int>.Some(1), "LeftValue"))])
            );        
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        var expected = new Insert<int, JoinedRecord>(1, new JoinedRecord("LeftValue", Option<string>.Some("RightValue")));
        
        Assert.Equal(expected, messages.Single().Value.Value.Changes.Single());
    }
    
    [Fact]
    public void RightRecordInsertedAfterMatchingLeftRow_EmitsUnjoinedRowInsertBeforeJoinedRowUpdate()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            
            var leftTableStream = testScheduler.CreateColdIndexedTableStream(
                (0, [new Insert<int, LeftRecord>(1, new LeftRecord(Option<int>.Some(1), "LeftValue"))])); 
            
            var rightTableStream = testScheduler.CreateColdIndexedTableStream(
                (10, [new Insert<int, RightRecord>(1, new RightRecord("RightValue"))])
            );
            
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        Assert.Equal(2, messages.Count);
        Assert.Equal(
            new Insert<int, JoinedRecord>(1, new JoinedRecord("LeftValue", Option<string>.None)), 
            messages[0].Value.Value.Changes.Single());
        Assert.Equal(
            new Update<int, JoinedRecord>(1, new JoinedRecord("LeftValue", Option<string>.None), new JoinedRecord("LeftValue", Option<string>.Some("RightValue"))), 
            messages[1].Value.Value.Changes.Single());
    }
    
    [Fact]
    public void RightRecordInsertedAfterMultipleMatchingLeftRows_EmitsJoinedRowUpdatesForAllMatchingRowsInSingleBatch()
    {
        var messages = TestHelper.RunScheduledTableStreamFixture(testScheduler =>
        {
            var leftTableStream = testScheduler.CreateColdIndexedTableStream(
                (0, [
                    // matched
                    new Insert<int, LeftRecord>(1, new LeftRecord(Option<int>.Some(1), "LeftValue1")),
                    new Insert<int, LeftRecord>(2, new LeftRecord(Option<int>.Some(1), "LeftValue2")),
                    
                    // unmatched
                    new Insert<int, LeftRecord>(3, new LeftRecord(Option<int>.Some(int.MaxValue), "LeftValue3")),
                ])); 
            
            var rightTableStream = testScheduler.CreateColdIndexedTableStream(
                (10, [new Insert<int, RightRecord>(1, new RightRecord("RightValue"))])
            );
            
            return leftTableStream
                .LeftJoin(rightTableStream, record => record.ForeignKey, (leftRecord, optionOfRightRecord) => new JoinedRecord(leftRecord, optionOfRightRecord));
        });
        
        Assert.Equal(2, messages.Count);
        Assert.Equal(2, messages[1].Value.Value.Changes.Length);
        Assert.Equal(
            new Update<int, JoinedRecord>(1, new JoinedRecord("LeftValue1", Option<string>.None), new JoinedRecord("LeftValue1", Option<string>.Some("RightValue"))), 
            messages[1].Value.Value.Changes[0]);
        Assert.Equal(
            new Update<int, JoinedRecord>(2, new JoinedRecord("LeftValue2", Option<string>.None), new JoinedRecord("LeftValue2", Option<string>.Some("RightValue"))), 
            messages[1].Value.Value.Changes[1]);
    }
}
