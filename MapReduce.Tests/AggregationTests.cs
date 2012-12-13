using System;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture]
    public class AggregationTests
    {
        [Test]
        public void Aggregate()
        {
            var dataBase = new MongoClient("mongodb://localhost").GetServer().GetDatabase("MapReduce");
            var collection = dataBase.GetCollection<Session>("Sessions");
            var match = new BsonDocument
            {
                {
                    "$match",
                    new BsonDocument
                    {
                        { "StartDate", new BsonDocument { { "$lt", DateTime.Parse("2011-01-02") } }},
                        { "EndDate", new BsonDocument { { "$gt", DateTime.Parse("2011-01-01") } }}
                    }
                }
            };

            var project = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument
                    {
                        { "_id", 0 },
                        { "start",
                            new BsonDocument
                            {
                                { "year",  new BsonDocument { { "$year", "$StartDate" } } },
                                { "month",  new BsonDocument { { "$month", "$StartDate" } } },
                                { "day",  new BsonDocument { { "$dayOfMonth", "$StartDate" } } },
                                { "hour",  new BsonDocument { { "$hour", "$StartDate" } } },
                                { "minute",  new BsonDocument { { "$minute", "$StartDate" } } }
                            }
                        },
                        { "end",
                            new BsonDocument
                            {
                                { "year",  new BsonDocument { { "$year", "$EndDate" } } },
                                { "month",  new BsonDocument { { "$month", "$EndDate" } } },
                                { "day",  new BsonDocument { { "$dayOfMonth", "$EndDate" } } },
                                { "hour",  new BsonDocument { { "$hour", "$EndDate" } } },
                                { "minute",  new BsonDocument { { "$minute", "$EndDate" } } }
                            }
                        }
                    }
                }
            };

            var group = new BsonDocument
            {
                {
                    "$group",
                    new BsonDocument
                    {
                        { "_id",
                            new BsonDocument
                            {
                                { "syear", "$start.year" },
                                { "smonth", "$start.month" }
                            }
                        }
                    }
                }
            };

            var pipeline = new[] { match, project };
            var result = collection.Aggregate(pipeline);
            var matching = result.ResultDocuments.Count();
        }
    }
}