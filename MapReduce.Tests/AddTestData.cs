using System;
using NUnit.Framework;

namespace MapReduce.Tests
{
    [TestFixture]
    public class AddTestData
    {
        private Random hour = new Random(23);
        private Random minute = new Random(59);
        private Random second = new Random(59);

        private const int minimumDuration = 1;
        private const int maximumDuration = 360;

        [Test, Ignore]
        public void GenerateSessions()
        {
            var startDate = DateTime.Parse("2011.01.01");
            var endDate = DateTime.Parse("2012.12.31");
            int maxSessions = 50;

            this.GenerateSessions(startDate, endDate, maxSessions);
        }

        private void GenerateSessions(DateTime startDate, DateTime endDate, int maxSessions)
        {
            var date = startDate;

            ISessionRepository repository = new SessionRepository();
            while (date <= endDate)
            {
                for (int i = 0; i < maxSessions; i++)
                {
                    var session = this.GenerateSession(date);
                    repository.Add(session);
                }
                date = date.AddDays(1);
            }
        }

        private Session GenerateSession(DateTime date)
        {
            var startDate = date;

            var h = hour.Next(0, 22);
            var m = minute.Next(0, 58);
            var s = second.Next(0, 58);
            var t = new TimeSpan(h, m, s);

            startDate = startDate.Add(t);

            var endDate = date;

            h = hour.Next(h + 1, 23);
            m = minute.Next(m + 1, 59);
            s = second.Next(s + 1, 59);
            t = new TimeSpan(h, m, s);

            endDate = endDate.Add(t);
            return new Session(startDate, endDate);
        }
    }
}