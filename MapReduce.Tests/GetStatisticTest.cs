using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;

namespace MapReduce.Tests
{
    public class GetStatisticTest
    {
        private static TimeSpan delta = new TimeSpan(0, 5, 0);

        [Test, Ignore]
        public void GetPlainTest()
        {
            var startDate = DateTime.Parse("2012.12.07 00:00:00");
            var endDate = DateTime.Parse("2012.12.07 23:59:59");

            var duration = new TimeSpan(endDate.Ticks - startDate.Ticks);
            var maxCount = duration.TotalMinutes / delta.TotalMinutes;

            var repository = new SessionRepository();
            var sessions = repository.Get();

            var statistic = new List<StatisticItem>();
            for (int i = 0; i < maxCount; i++)
            {
                var startStroke = startDate.AddMinutes(delta.Minutes * i);
                var endStroke = startStroke.AddMinutes(delta.Minutes);
                statistic.Add(this.GetStatisticItem(startStroke, endStroke, sessions));
            }

            foreach (var statItem in statistic)
            {
                Trace.WriteLine(statItem.DateTime + " " + statItem.Count);
            }
        }

        public void GetDbTest()
        { 

        }

        private StatisticItem GetStatisticItem(DateTime startStroke, DateTime endStroke, IEnumerable<Session> sessions)
        {
            var statisticItem = new StatisticItem();

            statisticItem.DateTime = startStroke;
            statisticItem.Count = sessions.Where(session => session.StartDate < endStroke && session.EndDate > startStroke).Count();

            return statisticItem;
        }
    }
}