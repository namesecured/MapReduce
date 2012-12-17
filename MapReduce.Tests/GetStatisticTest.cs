using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
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

        [Test]
        public void GetWithLinqAggregation()
        {
            var dataBase = new MongoClient("mongodb://localhost").GetServer().GetDatabase("MapReduce");
            var collection = dataBase.GetCollection<Session>("Sessions");
            IMongoQuery[] queryList = new IMongoQuery[2];

            var startDate = new DateTime(2011, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var endDate = new DateTime(2012, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            queryList[0] = Query.LT("StartDate", endDate);
            queryList[1] = Query.GT("EndDate", startDate);
            var sessions = collection.Find(Query.And(queryList)).ToList();

            var duration = new TimeSpan(endDate.Ticks - startDate.Ticks);
            var maxCount = duration.TotalMinutes / delta.TotalMinutes;

            List<Tuple<DateTime, DateTime>> pairs = new List<Tuple<DateTime, DateTime>>();

            for (int i = 0; i < maxCount; i++)
            {
                var startStroke = startDate.AddMinutes(delta.Minutes * i);
                var endStroke = startStroke.AddMinutes(delta.Minutes);
                pairs.Add(new Tuple<DateTime, DateTime>(startStroke, endStroke));
            }

            // Sequental(sessions, pairs);
            var parallel = Parallel(sessions, pairs);
            var parallelForceParallelism = ParallelForceParallelism(sessions, pairs);
            var parallelOrdered = ParallelOrdered(sessions, pairs);
            var parallelOrderedForceParallelism = ParallelOrderedForceParallelism(sessions, pairs);

            Assert.IsTrue(parallel.Count == parallelForceParallelism.Count 
                && parallelOrdered.Count == parallelOrderedForceParallelism.Count
                && parallel.Count == parallelOrderedForceParallelism.Count
                && parallelOrdered.Count == parallelForceParallelism.Count);

            for (int i = 0; i < parallel.Count; i++)
            {
                if (parallel[i].StartDate == parallelForceParallelism[i].StartDate
                    && parallelOrdered[i].StartDate == parallelOrderedForceParallelism[i].StartDate
                    && parallel[i].StartDate == parallelOrderedForceParallelism[i].StartDate
                    && parallelOrdered[i].StartDate == parallelForceParallelism[i].StartDate)
                {
                    continue;
                }

                Trace.WriteLine("NOT EQUALS: " + i);
            }

            // Parallel2(sessions, pairs);

            return;
        }

        private static List<StatisticItem> ParallelOrderedForceParallelism(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().AsOrdered().WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> ParallelForceParallelism(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().WithExecutionMode(ParallelExecutionMode.ForceParallelism).WithDegreeOfParallelism(2)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> ParallelOrderedFullBuffered(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().AsOrdered().WithMergeOptions(ParallelMergeOptions.FullyBuffered).WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> ParallelFullBuffered(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().WithMergeOptions(ParallelMergeOptions.FullyBuffered).WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> ParallelOrdered(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().AsOrdered()
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> Parallel(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().WithDegreeOfParallelism(6)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating " + MethodBase.GetCurrentMethod().Name);
            var items = new List<StatisticItem>();
            foreach (var i in q1)
            {
                items.Add(new StatisticItem { DateTime = i.TimeStamp, Count = i.Total });
            }
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static List<StatisticItem> Parallel2(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs.AsParallel().WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating parallel");
            var items = new List<StatisticItem>();
            q1.ForAll(q => items.Add(new StatisticItem { DateTime = q.TimeStamp, Count = q.Total }));
            watch.Stop();
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Parallel:\t" + watch.ElapsedTicks);
            return items;
        }

        private static void Sequental(List<Session> sessions, List<Tuple<DateTime, DateTime>> pairs)
        {
            var watch = new Stopwatch();
            watch.Start();
            var q1 = from pair in pairs
                     from session in sessions
                     where session.StartDate < pair.Item2 && session.EndDate > pair.Item1
                     group session by pair.Item1 into d
                     select new
                     {
                         TimeStamp = d.Key,
                         Total = d.Count()
                     };

            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Begin calculating Sequental");
            foreach (var i in q1)
            {
                var a = q1.ToList();
            }
            Trace.WriteLine(Thread.CurrentThread.ManagedThreadId + ": Sequental:\t" + watch.ElapsedTicks);
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