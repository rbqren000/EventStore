using System;
using System.Diagnostics;
using EventStore.Core.TransactionLog.Chunks;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount.ReadRangeAndNextEventNumber {
	public class
		when_reading_very_long_stream_with_max_age_and_mostly_expired_events : ReadIndexTestScenario {
		public when_reading_very_long_stream_with_max_age_and_mostly_expired_events() : base(
			maxEntriesInMemTable: 500_000, chunkSize: TFConsts.ChunkSize) {
		}

		protected override void WriteTestScenario() {
			var now = DateTime.UtcNow;
			var metadata = string.Format(@"{{""$maxAge"":{0}}}", (int)TimeSpan.FromMinutes(20).TotalSeconds);
			WriteStreamMetadata("ES", 0, metadata, now.AddMinutes(-100));
			for (int i = 0; i < 1_000_000; i++) {
				WriteSingleEvent("ES", i, "bla", now.AddMinutes(-50), retryOnFail: true);
			}

			for (int i = 1_000_000; i < 1_000_015; i++) {
				WriteSingleEvent("ES", i, "bla", now.AddMinutes(-1), retryOnFail: true);
			}
		}

		[Test, Explicit, Category("LongRunning")]
		public void on_read_from_beginning() {
			Stopwatch sw = Stopwatch.StartNew();
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 10);
			var elapsed = sw.Elapsed;

			Assert.AreEqual(1_000_000, res.NextEventNumber);
			Assert.AreEqual(0, res.Records.Length);
			Assert.AreEqual(false, res.IsEndOfStream);

			res = ReadIndex.ReadStreamEventsForward("ES", res.NextEventNumber, 10);

			Assert.AreEqual(1_000_010, res.NextEventNumber);
			Assert.AreEqual(10, res.Records.Length);
			Assert.AreEqual(false, res.IsEndOfStream);

			res = ReadIndex.ReadStreamEventsForward("ES", res.NextEventNumber, 10);

			Assert.AreEqual(1_000_015, res.NextEventNumber);
			Assert.AreEqual(5, res.Records.Length);
			Assert.AreEqual(true, res.IsEndOfStream);

			Assert.Less(elapsed, TimeSpan.FromSeconds(1));
		}
	}
}
