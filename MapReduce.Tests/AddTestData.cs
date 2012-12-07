using System;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture, Ignore]
    public class AddTestData
    {
        private Random hour = new Random(23);
        private Random minute = new Random(59);
        private Random second = new Random(59);

        private const int minimumDuration = 1;
        private const int maximumDuration = 360;

        [Test]
        public void GenerateSessions()
        {
            ISessionRepository repository = new SessionRepository();
            for (int i = 0; i < 50; i++)
            {
                var session = this.GenerateSession();
                repository.Add(session);
            }
        }

        private Session GenerateSession()
        {
            var startDate = DateTime.Parse("2012.12.07");

            var h = hour.Next(0, 22);
            var m = minute.Next(0, 58);
            var s = second.Next(0, 58);
            var t = new TimeSpan(h, m, s);

            startDate = startDate.Add(t);

            var endDate = DateTime.Parse("2012.12.07");

            h = hour.Next(h + 1, 23);
            m = minute.Next(m + 1, 59);
            s = second.Next(s + 1, 59);
            t = new TimeSpan(h, m, s);

            endDate = endDate.Add(t);
            return new Session(startDate, endDate);
        }
    }
}