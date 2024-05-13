// using System.Reactive.Linq;
//
// namespace TableStreams.Tests.Examples;
//
// public class LeftJoinTests
// {
//     const string MissingValueString = "N/A";
//     readonly record struct JoinedEmployee(string EmployeeId, string Name, string ManagerName);
//     
//     [Fact]
//     public async Task LeftJoin_WithPartiallyMatchingRightRows_OutputsAppropriateMatches()
//     {
//         var aggregatedUpdate = await TestData.CompanyStructure.Employees.AsIndexedTableStream()
//             .Publish(publishedCompanyStructureTableStream =>
//                 publishedCompanyStructureTableStream
//                     .LeftJoin(
//                         publishedCompanyStructureTableStream,
//                         x => x.ManagerEmployeeId,
//                         (employee, managerOption) => new JoinedEmployee(employee.EmployeeId, employee.Name,
//                             managerOption.Select(x => x.Name).IfNone(MissingValueString))))
//             .Aggregate()
//             .UnderlyingStream;
//
//         var joinedEmployees = aggregatedUpdate.Index.Values;
//         
//         var expectedOutput = new JoinedEmployee[]
//         {
//             new("E001", "Grande Fromage", MissingValueString),
//
//             new("E002", "Andy Assistant", "Grande Fromage"),
//             new("E003", "Marcie Manager", "Grande Fromage"),
//
//             new("E010", "Wanetta Worker", "Marcie Manager"),
//             new("E011", "Wilberforce Worker", "Marcie Manager"),
//         };
//         
//         Assert.Equivalent(expectedOutput.OrderBy(x=>x.EmployeeId), joinedEmployees.OrderBy(x=>x.EmployeeId));
//     }
//     
//     [Fact]
//     public async Task LeftJoin_WhenMatchingRows_OutputsAppropriateMatches()
//     {
//         var expectedOutput = new JoinedEmployee[]
//         {
//             new("E001", "Grande Fromage", MissingValueString),
//
//             new("E002", "Andy Assistant", "Grande Fromage"),
//             new("E003", "Marcie Manager", "Grande Fromage"),
//
//             new("E010", "Wanetta Worker", "Marcie Manager"),
//             new("E011", "Wilberforce Worker", "Marcie Manager"),
//         };
//         
//         var aggregatedUpdate = await TestData.CompanyStructure.Employees.AsIndexedTableStream()
//             .Publish(publishedCompanyStructureTableStream => 
//                 publishedCompanyStructureTableStream
//                     .LeftJoin(
//                         publishedCompanyStructureTableStream, 
//                         x => x.ManagerEmployeeId,
//                         (employee, managerOption) => new JoinedEmployee(employee.EmployeeId, employee.Name, managerOption.Select(x => x.Name).IfNone(MissingValueString))))
//             .Aggregate()
//             .UnderlyingStream;
//
//         var joinedEmployees = aggregatedUpdate.Index.Values;
//
//         Assert.Equivalent(expectedOutput.OrderBy(x=>x.EmployeeId), joinedEmployees.OrderBy(x=>x.EmployeeId));
//     }
// }