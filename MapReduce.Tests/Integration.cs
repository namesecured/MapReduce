using MongoDB.Bson;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture]
    public class Integration
    {
        [Test, Ignore]
        public void Should_connects_to_the_server()
        {
            var mongoDataProvider = new MongoDataProvider();
            mongoDataProvider.Connect();
            mongoDataProvider.Disconnect();
        }
    }
}