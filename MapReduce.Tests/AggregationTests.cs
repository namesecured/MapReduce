using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture]
    public class AggregationTests
    {
        private static TimeSpan delta = new TimeSpan(0, 5, 0);

        [Test]
        public void Aggregate()
        {
            var dataBase = new MongoClient("mongodb://localhost").GetServer().GetDatabase("MapReduce");
            var collection = dataBase.GetCollection<Session>("Sessions");

            var match = GetMatchOperator(GetStartDate(), GetEndDate());
            var project = GetProjectOperator(GetStartDate());

            var pipeline = new[] { match, project };
            var result = collection.Aggregate(pipeline);
            var matching = result.ResultDocuments.Count();
        }

        [Test]
        public void Aggregate_with_map_reduce()
        {            
            var dataBase = new MongoClient("mongodb://localhost").GetServer().GetDatabase("MapReduce");
            var sessions = dataBase.GetCollection<Session>("Sessions");            
            
            BsonJavaScript map = new BsonJavaScript(Scipts.map);
            BsonJavaScript reduce = new BsonJavaScript(Scipts.reduce);
            var result = sessions.MapReduce(map, reduce);            
        }

        [Test]
        public void Aggregate_with_query()
        {
            var startDate = GetStartDate();
            var endDate = GetEndDate();

            var duration = new TimeSpan(endDate.Ticks - startDate.Ticks);
            var maxCount = duration.TotalMinutes / delta.TotalMinutes;

            var stopWatch = new Stopwatch();
            var dataBase = new MongoClient("mongodb://localhost").GetServer().GetDatabase("MapReduce");
            var sessions = dataBase.GetCollection<Session>("Sessions");

            stopWatch.Start();
            var statistic = new List<StatisticItem>();

            for (int i = 0; i < maxCount; i++)
            {
                var startStroke = startDate.AddMinutes(delta.Minutes * i);
                var endStroke = startStroke.AddMinutes(delta.Minutes);
                statistic.Add(this.GetStatisticItem(startStroke, endStroke, sessions));
            }
            stopWatch.Stop();
            foreach (var statItem in statistic)
            {
                Trace.WriteLine(statItem.DateTime + " " + statItem.Count);
            }
            Trace.WriteLine("Aggregation: " + stopWatch.ElapsedTicks);
        }

        private StatisticItem GetStatisticItem(DateTime startStroke, DateTime endStroke, MongoCollection<Session> sessions)
        {
            var match = GetMatchOperator(startStroke, endStroke);
            var project = GetProjectOperator(startStroke);
            var project1 = GetProjectOperator1();
            var group = GetGroupOperator();
            var pipeline = new[] { match, project, group, project1 };
            var queryResult = sessions.Aggregate(pipeline);
            var items = queryResult.ResultDocuments.ToList();

            var doc = queryResult.ResultDocuments.Select(BsonSerializer.Deserialize<StatisticItem>).FirstOrDefault();
            var count = doc == null ? 0 : doc.Count;
            var result = new StatisticItem { DateTime = startStroke, Count = count };
            return result;
        }

        private static BsonDocument GetProjectOperator2()
        {
            var project = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument
                    {
                        { "_id", 0 },
                        { "DateTime", "$StartDate" },
                        { "StartDate", "$StartDate" },
                        { "EndDate", "$EndDate" },
                        { "SessionId", "$SessionId" },
                    }
                }
            };
            return project;
        }

        private static BsonDocument GetProjectOperator1()
        {
            var project = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument
                    {
                        { "_id", 0 },
                        { "DateTime", "$DateTime" },
                        { "Count", "$Count"}
                    }
                }
            };
            return project;
        }

        private static DateTime GetEndDate()
        {
            return new DateTime(2011, 1, 1, 22, 50, 0, DateTimeKind.Utc);
        }

        private static DateTime GetStartDate()
        {
            return new DateTime(2011, 1, 1, 20, 45, 0, DateTimeKind.Utc);
        }

        private static BsonDocument GetGroupOperator()
        {
            var group = new BsonDocument
            {
                {
                    "$group",
                    new BsonDocument
                    {
                        { "_id",
                            new BsonDocument
                            {
                                { "DateTime", "$DateTime" }
                            }
                        },
                        {
                            "Count",
                            new BsonDocument
                            {
                                { "$sum", 1 }
                            }
                        }
                    }
                }
            };
            return group;
        }

        private static BsonDocument GetProjectOperator(DateTime startDate)
        {
            var project = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument
                    {
                        { "_id", 0 },
                        { "DateTime", new BsonDocument { { "$toLower", startDate.ToString() } } },
                    }
                }
            };
            return project;
        }

        private static BsonDocument GetMatchOperator(DateTime startDate, DateTime endDate)
        {
            var result = new BsonDocument
            {
                {
                    "$match",
                    new BsonDocument
                    {
                        { "StartDate", new BsonDocument { { "$lt", endDate } }},
                        { "EndDate", new BsonDocument { { "$gt", startDate } }}
                    }
                }
            };

            return result;
        }

        private static BsonDocument GetSumOperator()
        {
            var sum = new BsonDocument
            {
                {
                    "number",
                    new BsonDocument
                    {
                        { "$sum", 1 }
                    }
                }
            };

            return sum;
        }
    }
}