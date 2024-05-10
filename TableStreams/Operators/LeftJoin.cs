using System.Collections.Immutable;
using System.Diagnostics;
using System.Reactive.Linq;
using LanguageExt;
using LanguageExt.SomeHelp;

namespace TableStreams.Operators;

internal static class LeftJoinOperator
{
    internal static IIndexedTableStream<TLeftKey, TResult>
        LeftJoin<TLeftKey, TLeftValue, TRightKey, TRightValue, TResult>(
            IIndexedTableStream<TLeftKey, TLeftValue> leftSource,
            IIndexedTableStream<TRightKey, TRightValue> rightSource,
            Func<TLeftValue, Option<TRightKey>> foreignKeyExtractor,
            Func<TLeftValue, Option<TRightValue>, TResult> resultSelector)
        where TLeftKey : notnull where TRightKey : notnull
    {
        var resultStream = Observable.Create<IndexedTableStreamUpdate<TLeftKey, TResult>>(
            observer =>
            {
                var state = new LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue>(
                    ImmutableDictionary<TLeftKey, TResult>.Empty,
                    ImmutableDictionary<TRightKey, Dictionary<TLeftKey, TLeftValue>>.Empty,
                    ImmutableDictionary<TRightKey, TRightValue>.Empty);

                return
                    // ReSharper disable once InvokeAsExtensionMethod - Readability
                    Observable.Merge(
                            leftSource.UnderlyingStream.Select(Either<IndexedTableStreamUpdate<TLeftKey, TLeftValue>, IndexedTableStreamUpdate<TRightKey, TRightValue>>.Left),
                            rightSource.UnderlyingStream.Select(Either<IndexedTableStreamUpdate<TLeftKey, TLeftValue>, IndexedTableStreamUpdate<TRightKey, TRightValue>>.Right)
                        )
                        .Subscribe(eitherLeftOrRightUpdate =>
                            {
                                var initialState = state;
                                
                                // intermediate row changes have sufficient information to both build the output rows and recude internal state
                                // perhaps the intermediate state can be extracted since it's a distinct abstraction but that might impact coherence
                                
                                var intermediateRowChanges = eitherLeftOrRightUpdate.Match(
                                    right => DeriveIntermediateChangesFromRightChange(initialState, right, resultSelector),
                                    left => DeriveIntermediateChangesFromLeftChange(initialState, left, resultSelector, foreignKeyExtractor));

                                if (intermediateRowChanges.Length == 0)
                                {
                                    return;
                                }
                                
                                state = ReduceState(
                                    state,
                                    eitherLeftOrRightUpdate.Match(
                                        right => right.Index.ToSome(), 
                                        _ => Option<IReadOnlyDictionary<TRightKey, TRightValue>>.None),
                                    intermediateRowChanges);

                                var changesForPublication = TransformIntermediateToPublishable(intermediateRowChanges);
                                
                                observer.OnNext(new IndexedTableStreamUpdate<TLeftKey, TResult>(
                                    state.ResultTableIndex, 
                                    changesForPublication
                                    ));
                            },
                            observer.OnError,
                            observer.OnCompleted);
            });

        return new IndexedTableStream<TLeftKey, TResult>(resultStream);
    }

     readonly record struct LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue>(
        // this is the materialized index that gets updated and published alongside every update
        ImmutableDictionary<TLeftKey, TResult> ResultTableIndex, 
        
        // used to identify which joined records need to be serviced when a right-side update is processed
        // maintained internally
        ImmutableDictionary<TRightKey, Dictionary<TLeftKey, TLeftValue>> RightToLeftJoinIndex,
        
        // used to attempt joins
        // sourced from latest right-side update
        IReadOnlyDictionary<TRightKey, TRightValue> RightTableIndex) where TLeftKey : notnull where TRightKey : notnull;

    static LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue> ReduceState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue>(
        LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue> state,
        Option<IReadOnlyDictionary<TRightKey, TRightValue>> updatedRightIndex,
        TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>[] diffs) where TLeftKey : notnull where TRightKey : notnull
    {
        var resultTableIndexBuilder = state.ResultTableIndex.ToBuilder();
        var rightToLeftJoinIndexBuilder = state.RightToLeftJoinIndex.ToBuilder();
        
        foreach (var diff in diffs)
        {
            ApplyDiffToResultIndexBuilder(resultTableIndexBuilder, diff);
            ApplyDiffToRightToLeftJoinIndexBuilder(rightToLeftJoinIndexBuilder, diff);
        }

        return new LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue>(
            resultTableIndexBuilder.ToImmutable(),
            rightToLeftJoinIndexBuilder.ToImmutable(),
            updatedRightIndex.IfNone(state.RightTableIndex));
    }

    static void ApplyDiffToResultIndexBuilder<TLeftKey, TResult, TLeftValue, TRightKey, TRightValue>(
        ImmutableDictionary<TLeftKey, TResult>.Builder resultTableIndexBuilder, 
        TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)> diff)
        where TLeftKey : notnull where TRightKey : notnull
    {
        diff.Match(
            insert =>
            {
                resultTableIndexBuilder[insert.Key] = insert.InsertedValue.Result;
            },
            update =>
            {
                resultTableIndexBuilder[update.Key] = update.UpdatedValue.Result;
            },
            delete =>
            {
                resultTableIndexBuilder.Remove(delete.Key);
            });
    }
    
    internal static void ApplyDiffToRightToLeftJoinIndexBuilder<TRightKey, TLeftKey, TLeftValue, TRightValue, TResult>(
        ImmutableDictionary<TRightKey, Dictionary<TLeftKey, TLeftValue>>.Builder joinedRowsIndexBuilder, 
        TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)> diff) where TRightKey : notnull where TLeftKey : notnull
    {
        diff.Match(
            insert =>
            {
                insert.InsertedValue.RightKey.IfSome(rightKey => SetJoin(joinedRowsIndexBuilder, rightKey, insert.Key, insert.InsertedValue.LeftValue));
            },
            update =>
            {
                if (!update.PreviousValue.RightKey.Equals(update.UpdatedValue.RightKey) || !update.PreviousValue.LeftValue!.Equals(update.UpdatedValue.LeftValue))
                {
                    update.PreviousValue.RightKey.IfSome(previousRightKey => ClearJoin(joinedRowsIndexBuilder, previousRightKey, update.Key));

                    update.UpdatedValue.RightKey.IfSome(rightKey => SetJoin(joinedRowsIndexBuilder, rightKey, update.Key, update.UpdatedValue.LeftValue));
                }
            },
            delete =>
            {
                delete.DeletedValue.RightKey.IfSome(previousRightKey => ClearJoin(joinedRowsIndexBuilder, previousRightKey, delete.Key)  );
            });

        void SetJoin(ImmutableDictionary<TRightKey, Dictionary<TLeftKey, TLeftValue>>.Builder builder, TRightKey rightKey, TLeftKey leftKey, TLeftValue leftValue)
        {
            builder.GetOrAdd(rightKey, () => new Dictionary<TLeftKey, TLeftValue>())[leftKey] = leftValue;
        }
        
        void ClearJoin(ImmutableDictionary<TRightKey, Dictionary<TLeftKey, TLeftValue>>.Builder builder, TRightKey rightKey, TLeftKey leftKey)
        {
            var dict = builder[rightKey];

            if (dict.Count == 1)
            {
                joinedRowsIndexBuilder.Remove(rightKey);
            }
            else
            {
                dict.Remove(leftKey);
            }
        }
    }

    static TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>[] DeriveIntermediateChangesFromLeftChange
        <TLeftKey, TResult, TRightKey, TRightValue, TLeftValue>(
        LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue> state,
        IndexedTableStreamUpdate<TLeftKey, TLeftValue> leftUpdate,
        Func<TLeftValue, Option<TRightValue>, TResult> resultSelector,
        Func<TLeftValue, Option<TRightKey>> foreignKeyExtractor) where TLeftKey : notnull where TRightKey : notnull
    {
        return leftUpdate.Changes.Select(change => change.Match(insert =>
                {
                    // a left-side insert is always a result stream insert
                    // its foreign key may match something from the existing right-side index

                    var foreignKeyOption = foreignKeyExtractor(insert.InsertedValue);
                    var rightSideOption = foreignKeyOption.Bind(x => state.RightTableIndex.GetValueOption(x));
                    var resultValue = resultSelector(insert.InsertedValue, rightSideOption);

                    return [new Insert<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>(
                        insert.Key, 
                        (insert.InsertedValue, foreignKeyOption, rightSideOption, resultValue))];
                },
                update =>
                {
                    // the left record has changed. the foreign key might also have changed

                    var previousForeignKeyOption = foreignKeyExtractor(update.PreviousValue);
                    var previousRightSideOption = previousForeignKeyOption.Bind(x => state.RightTableIndex.GetValueOption(x));
                    var previousResultValue = state.ResultTableIndex[update.Key];

                    var foreignKeyOption = foreignKeyExtractor(update.UpdatedValue);
                    var rightSideOption = foreignKeyOption.Bind(x => state.RightTableIndex.GetValueOption(x));
                    var resultValue = resultSelector(update.UpdatedValue, rightSideOption);
                    
                    return [new Update<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>(
                        update.Key,
                        (update.PreviousValue,  previousForeignKeyOption, previousRightSideOption, previousResultValue),
                        (update.UpdatedValue, foreignKeyOption, rightSideOption, resultValue))];
                },
                delete =>
                {
                    // this is a delete in the result stream

                    var previousForeignKeyOption = foreignKeyExtractor(delete.DeletedValue);
                    var previousRightSideOption = previousForeignKeyOption.Bind(x => state.RightTableIndex.GetValueOption(x));
                    var previousResultValue = state.ResultTableIndex[delete.Key];
                    
                    return new TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>[]
                    {
                        new Delete<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>(
                            delete.Key, 
                            (delete.DeletedValue,  previousForeignKeyOption, previousRightSideOption, previousResultValue))
                    };
                }))
            .Flatten()
            .ToArray();
    }
    
    static TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>[] DeriveIntermediateChangesFromRightChange<TLeftKey, TResult, TRightKey, TRightValue, TLeftValue>(
        LeftJoinState<TLeftKey, TLeftValue, TResult, TRightKey, TRightValue> state, 
        IndexedTableStreamUpdate<TRightKey, TRightValue> rightUpdate, 
        Func<TLeftValue, Option<TRightValue>, TResult> resultSelector) where TLeftKey : notnull where TRightKey : notnull
    {
        return rightUpdate.Changes.Select(change => change.Match(insert =>
                {
                    // a right-side insert might match left rows which have previously joined, or tried to join with this key
                    // if we do match any left rows, they can only be updates in the result row since they must have already been emitted

                    return state.RightToLeftJoinIndex.GetValueOption(insert.Key)
                        .Match(
                            matchedLeftRows => matchedLeftRows.Select(matchedLeftRow =>
                                {
                                    var previousResultValue = state.ResultTableIndex[matchedLeftRow.Key];
                                    var resultValue = resultSelector(matchedLeftRow.Value, insert.InsertedValue);
                                    
                                    return new Update<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey,
                                        Option<TRightValue> RightValue, TResult Result)>(
                                        matchedLeftRow.Key,
                                        (matchedLeftRow.Value, insert.Key, Option<TRightValue>.None, previousResultValue),
                                        (matchedLeftRow.Value, insert.Key, insert.InsertedValue, resultValue));
                                })
                                .Cast<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>()
                                .ToArray(),
                            Array.Empty<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>);
                },
                update =>
                {
                    // a right-side update will trigger an update for all joined rows, with the new right value

                    return state.RightToLeftJoinIndex.GetValueOption(update.Key)
                        .Match(
                            matchedLeftRows => matchedLeftRows.Select(matchedLeftRow =>
                                    new Update<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey,
                                        Option<TRightValue> RightValue, TResult Result)>(
                                        matchedLeftRow.Key,
                                        (matchedLeftRow.Value, update.Key, update.PreviousValue, state.ResultTableIndex[matchedLeftRow.Key]),
                                        (matchedLeftRow.Value, update.Key, update.UpdatedValue, resultSelector(matchedLeftRow.Value, update.UpdatedValue))))
                                .Cast<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>()
                                .ToArray(),
                            Array.Empty<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>);
                },
                delete =>
                {
                    // a right-side update will trigger an update for all joined rows, with will lose their right value

                    return state.RightToLeftJoinIndex.GetValueOption(delete.Key)
                        .Match(
                            matchedLeftRows => matchedLeftRows.Select(matchedLeftRow =>
                                    new Update<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey,
                                        Option<TRightValue> RightValue, TResult Result)>(
                                        matchedLeftRow.Key,
                                        (matchedLeftRow.Value, delete.Key, delete.DeletedValue, state.ResultTableIndex[matchedLeftRow.Key]),
                                        (matchedLeftRow.Value, delete.Key, Option<TRightValue>.None, resultSelector(matchedLeftRow.Value, Option<TRightValue>.None))))
                                .Cast<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>(),
                            Array.Empty<TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>>);

                }))
            .Flatten()
            .ToArray();
    }
    
    static TableRowChange<TLeftKey, TResult>[] TransformIntermediateToPublishable<TLeftKey, TResult, TLeftValue, TRightKey, TRightValue>(
        TableRowChange<TLeftKey, (TLeftValue LeftValue, Option<TRightKey> RightKey, Option<TRightValue> RightValue, TResult Result)>[] intermediateRowChanges) where TLeftKey : notnull where TRightKey : notnull
    {
        return intermediateRowChanges.Select(intermediateRowChange =>
            {
                return intermediateRowChange.Match<TableRowChange<TLeftKey, TResult>>(
                    insert => new Insert<TLeftKey, TResult>(insert.Key, insert.InsertedValue.Result),
                    update => new Update<TLeftKey, TResult>(update.Key, update.PreviousValue.Result, update.UpdatedValue.Result),
                    delete => new Delete<TLeftKey, TResult>(delete.Key, delete.DeletedValue.Result)
                );
            })
        .ToArray();
    }
}

