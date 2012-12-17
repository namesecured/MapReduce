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

        [Test]
        public void GetPlainTest()
        {
            var startDate = DateTime.Parse("2011-01-01 20:45:00");
            var endDate = DateTime.Parse("2011-01-01 22:50:00");

            var duration = new TimeSpan(endDate.Ticks - startDate.Ticks);
            var maxCount = duration.TotalMinutes / delta.TotalMinutes;

            var repository = new SessionRepository();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            stopWatch.Stop();
            Trace.WriteLine("Imperative all: " + stopWatch.ElapsedTicks);

            stopWatch.Start();
            var sessions = repository.Get((session) => session.StartDate < endDate && session.EndDate > startDate);

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
            Trace.WriteLine("Imperative: " + stopWatch.ElapsedTicks);
        }

        private StatisticItem GetStatisticItem(DateTime startStroke, DateTime endStroke, IEnumerable<Session> sessions)
        {
            var statisticItem = new StatisticItem();

            statisticItem.DateTime = startStroke;
            var items = sessions.Where(session => session.StartDate < endStroke && session.EndDate > startStroke).ToList();
            statisticItem.Count = sessions.Where(session => session.StartDate < endStroke && session.EndDate > startStroke).Count();

            return statisticItem;
        }
    }
}