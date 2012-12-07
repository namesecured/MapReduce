using MongoDB.Bson;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture, Ignore]
    public class Integration
    {
        [Test]
        public void Should_connects_to_the_server()
        {
            var mongoDataProvider = new MongoDataProvider();
            mongoDataProvider.Connect();
            mongoDataProvider.Disconnect();
        }
    }
}