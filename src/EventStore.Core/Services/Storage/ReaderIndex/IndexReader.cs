using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Threading;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Index;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.LogRecords;
using ILogger = Serilog.ILogger;

namespace EventStore.Core.Services.Storage.ReaderIndex {
	public interface IIndexReader {
		long CachedStreamInfo { get; }
		long NotCachedStreamInfo { get; }
		long HashCollisions { get; }

		IndexReadEventResult ReadEvent(string streamId, long eventNumber);
		IndexReadStreamResult ReadStreamEventsForward(string streamId, long fromEventNumber, int maxCount);
		IndexReadStreamResult ReadStreamEventsBackward(string streamId, long fromEventNumber, int maxCount);
		StorageMessage.EffectiveAcl GetEffectiveAcl(string streamId);
		/// <summary>
		/// Doesn't filter $maxAge, $maxCount, $tb(truncate before), doesn't check stream deletion, etc.
		/// </summary>
		PrepareLogRecord ReadPrepare(string streamId, long eventNumber);

		string GetEventStreamIdByTransactionId(long transactionId);

		StreamMetadata GetStreamMetadata(string streamId);
		long GetStreamLastEventNumber(string streamId);
	}

	public class IndexReader : IIndexReader {
		private static readonly ILogger Log = Serilog.Log.ForContext<IndexReader>();

		public long CachedStreamInfo {
			get { return Interlocked.Read(ref _cachedStreamInfo); }
		}

		public long NotCachedStreamInfo {
			get { return Interlocked.Read(ref _notCachedStreamInfo); }
		}

		public long HashCollisions {
			get { return Interlocked.Read(ref _hashCollisions); }
		}

		private readonly IIndexBackend _backend;
		private readonly ITableIndex _tableIndex;
		private readonly bool _skipIndexScanOnRead;
		private readonly StreamMetadata _metastreamMetadata;

		private long _hashCollisions;
		private long _cachedStreamInfo;
		private long _notCachedStreamInfo;
		private int _hashCollisionReadLimit;

		public IndexReader(IIndexBackend backend, ITableIndex tableIndex, StreamMetadata metastreamMetadata,
			int hashCollisionReadLimit, bool skipIndexScanOnRead) {
			Ensure.NotNull(backend, "backend");
			Ensure.NotNull(tableIndex, "tableIndex");
			Ensure.NotNull(metastreamMetadata, "metastreamMetadata");

			_backend = backend;
			_tableIndex = tableIndex;
			_metastreamMetadata = metastreamMetadata;
			_hashCollisionReadLimit = hashCollisionReadLimit;
			_skipIndexScanOnRead = skipIndexScanOnRead;
		}

		IndexReadEventResult IIndexReader.ReadEvent(string streamId, long eventNumber) {
			Ensure.NotNullOrEmpty(streamId, "streamId");
			if (eventNumber < -1) throw new ArgumentOutOfRangeException("eventNumber");
			using (var reader = _backend.BorrowReader()) {
				return ReadEventInternal(reader, streamId, eventNumber);
			}
		}

		private IndexReadEventResult ReadEventInternal(TFReaderLease reader, string streamId, long eventNumber) {
			var lastEventNumber = GetStreamLastEventNumberCached(reader, streamId);
			var metadata = GetStreamMetadataCached(reader, streamId);
			var originalStreamExists = OriginalStreamExists(reader, streamId);
			if (lastEventNumber == EventNumber.DeletedStream)
				return new IndexReadEventResult(ReadEventResult.StreamDeleted, metadata, lastEventNumber,
					originalStreamExists);
			if (lastEventNumber == ExpectedVersion.NoStream || metadata.TruncateBefore == EventNumber.DeletedStream)
				return new IndexReadEventResult(ReadEventResult.NoStream, metadata,lastEventNumber,
					originalStreamExists);
			if (lastEventNumber == EventNumber.Invalid)
				return new IndexReadEventResult(ReadEventResult.NoStream, metadata, lastEventNumber,
					originalStreamExists);

			if (eventNumber == -1)
				eventNumber = lastEventNumber;

			long minEventNumber = 0;
			if (metadata.MaxCount.HasValue)
				minEventNumber = Math.Max(minEventNumber, lastEventNumber - metadata.MaxCount.Value + 1);
			if (metadata.TruncateBefore.HasValue)
				minEventNumber = Math.Max(minEventNumber, metadata.TruncateBefore.Value);
			//TODO(clc): confirm this logic, it seems that reads less than min should be invaild rather than found
			if (eventNumber < minEventNumber || eventNumber > lastEventNumber)
				return new IndexReadEventResult(ReadEventResult.NotFound, metadata,  lastEventNumber,
					originalStreamExists);

			PrepareLogRecord prepare = ReadPrepareInternal(reader, streamId, eventNumber);
			if (prepare != null) {
				if (metadata.MaxAge.HasValue && prepare.TimeStamp < DateTime.UtcNow - metadata.MaxAge.Value)
					return new IndexReadEventResult(ReadEventResult.NotFound, metadata,  lastEventNumber,
						originalStreamExists);
				return new IndexReadEventResult(ReadEventResult.Success, new EventRecord(eventNumber, prepare),
					metadata, lastEventNumber, originalStreamExists);
			}

			return new IndexReadEventResult(ReadEventResult.NotFound, metadata, lastEventNumber,
				originalStreamExists: originalStreamExists);
		}

		PrepareLogRecord IIndexReader.ReadPrepare(string streamId, long eventNumber) {
			using (var reader = _backend.BorrowReader()) {
				return ReadPrepareInternal(reader, streamId, eventNumber);
			}
		}

		private PrepareLogRecord ReadPrepareInternal(TFReaderLease reader, string streamId, long eventNumber) {
			// we assume that you already did check for stream deletion
			Ensure.NotNullOrEmpty(streamId, "streamId");
			Ensure.Nonnegative(eventNumber, "eventNumber");

			return _skipIndexScanOnRead
				? ReadPrepareSkipScan(reader, streamId, eventNumber)
				: ReadPrepare(reader, streamId, eventNumber);
		}

		private PrepareLogRecord ReadPrepare(TFReaderLease reader, string streamId, long eventNumber) {
			var recordsQuery = _tableIndex.GetRange(streamId, eventNumber, eventNumber)
				.Select(x => new {x.Version, Prepare = ReadPrepareInternal(reader, x.Position)})
				.Where(x => x.Prepare != null && x.Prepare.EventStreamId == streamId)
				.GroupBy(x => x.Version).Select(x => x.Last()).ToList();
			if (recordsQuery.Count() == 1) {
				return recordsQuery.First().Prepare;
			}

			return null;
		}

		private PrepareLogRecord ReadPrepareSkipScan(TFReaderLease reader, string streamId, long eventNumber) {
			long position;
			if (_tableIndex.TryGetOneValue(streamId, eventNumber, out position)) {
				var rec = ReadPrepareInternal(reader, position);
				if (rec != null && rec.EventStreamId == streamId)
					return rec;

				foreach (var indexEntry in _tableIndex.GetRange(streamId, eventNumber, eventNumber)) {
					Interlocked.Increment(ref _hashCollisions);
					if (indexEntry.Position == position)
						continue;
					rec = ReadPrepareInternal(reader, indexEntry.Position);
					if (rec != null && rec.EventStreamId == streamId)
						return rec;
				}
			}

			return null;
		}

		protected static PrepareLogRecord ReadPrepareInternal(TFReaderLease reader, long logPosition) {
			RecordReadResult result = reader.TryReadAt(logPosition);
			if (!result.Success)
				return null;

			if (result.LogRecord.RecordType != LogRecordType.Prepare)
				throw new Exception(string.Format("Incorrect type of log record {0}, expected Prepare record.",
					result.LogRecord.RecordType));
			return (PrepareLogRecord)result.LogRecord;
		}

		IndexReadStreamResult IIndexReader.
			ReadStreamEventsForward(string streamId, long fromEventNumber, int maxCount) {
			return ReadStreamEventsForwardInternal(streamId, fromEventNumber, maxCount, _skipIndexScanOnRead);
		}

		private IndexReadStreamResult ReadStreamEventsForwardInternal(string streamId, long fromEventNumber,
			int maxCount, bool skipIndexScanOnRead) {
			Ensure.NotNullOrEmpty(streamId, "streamId");
			Ensure.Nonnegative(fromEventNumber, "fromEventNumber");
			Ensure.Positive(maxCount, "maxCount");

			using (var reader = _backend.BorrowReader()) {
				var lastEventNumber = GetStreamLastEventNumberCached(reader, streamId);
				var metadata = GetStreamMetadataCached(reader, streamId);
				if (lastEventNumber == EventNumber.DeletedStream)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.StreamDeleted,
						StreamMetadata.Empty, lastEventNumber);
				if (lastEventNumber == ExpectedVersion.NoStream || metadata.TruncateBefore == EventNumber.DeletedStream)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.NoStream, metadata,
						lastEventNumber);
				if (lastEventNumber == EventNumber.Invalid)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.NoStream, metadata,
						lastEventNumber);

				long startEventNumber = fromEventNumber;
				long endEventNumber = Math.Min(long.MaxValue, fromEventNumber + maxCount - 1);

				long minEventNumber = 0;
				if (metadata.MaxCount.HasValue)
					minEventNumber = Math.Max(minEventNumber, lastEventNumber - metadata.MaxCount.Value + 1);
				if (metadata.TruncateBefore.HasValue)
					minEventNumber = Math.Max(minEventNumber, metadata.TruncateBefore.Value);
				if (endEventNumber < minEventNumber)
					return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
						metadata, minEventNumber, lastEventNumber, isEndOfStream: false);
				startEventNumber = Math.Max(startEventNumber, minEventNumber);

				if (metadata.MaxAge.HasValue) {
					return ForStreamWithMaxAge(streamId,
						fromEventNumber, maxCount,
						startEventNumber, endEventNumber, lastEventNumber,
						metadata.MaxAge.Value, metadata, _tableIndex, reader, skipIndexScanOnRead);
				}

				var recordsQuery = _tableIndex.GetRange(streamId, startEventNumber, endEventNumber)
					.Select(x => new {x.Version, Prepare = ReadPrepareInternal(reader, x.Position)})
					.Where(x => x.Prepare != null && x.Prepare.EventStreamId == streamId);
				if (!skipIndexScanOnRead) {
					recordsQuery = recordsQuery.OrderByDescending(x => x.Version)
						.GroupBy(x => x.Version).Select(x => x.Last());
				}

				var records = recordsQuery.Reverse().Select(x => new EventRecord(x.Version, x.Prepare)).ToArray();

				long nextEventNumber = Math.Min(endEventNumber + 1, lastEventNumber + 1);
				if (records.Length > 0)
					nextEventNumber = records[records.Length - 1].EventNumber + 1;
				var isEndOfStream = endEventNumber >= lastEventNumber;
				return new IndexReadStreamResult(endEventNumber, maxCount, records, metadata,
					nextEventNumber, lastEventNumber, isEndOfStream);
			}
		}

		private IndexReadStreamResult ForStreamWithMaxAge(string streamId,
			long fromEventNumber, int maxCount, long startEventNumber,
			long endEventNumber, long lastEventNumber, TimeSpan maxAge, StreamMetadata metadata,
			ITableIndex tableIndex, TFReaderLease reader, bool skipIndexScanOnRead) {
			if (startEventNumber > lastEventNumber) {
				return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
					metadata, lastEventNumber + 1, lastEventNumber, isEndOfStream: true);
			}

			var ageThreshold = DateTime.UtcNow - maxAge;
			var nextEventNumber = lastEventNumber;
			var indexEntries = tableIndex.GetRange(streamId, startEventNumber, endEventNumber);

			//Move to the first valid entries. At this point we could instead return an empty result set with the minimum set, but that would 
			//involve an additional set of reads for no good reason
			while (indexEntries.Count == 0) {
				// this will generally only iterate once, unless a scavenge completes exactly now, in which case it might iterate twice
				if (tableIndex.TryGetOldestEntry(streamId, out var oldest)) {
					startEventNumber = oldest.Version;
					endEventNumber = startEventNumber + maxCount - 1;
					indexEntries = tableIndex.GetRange(streamId, startEventNumber, endEventNumber);
				} else {
					//scavenge completed and deleted our stream? return empty set and get the client to try again?
					return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
						metadata, lastEventNumber + 1, lastEventNumber, isEndOfStream: false);
				}
			}

			var results = new List<EventRecord>();
			for (int i = 0; i < indexEntries.Count; i++) {
				var prepare = ReadPrepareInternal(reader, indexEntries[i].Position);

				if (prepare == null || prepare.EventStreamId != streamId) {
					continue;
				}

				if (prepare?.TimeStamp >= ageThreshold) {
					results.Add(new EventRecord(indexEntries[i].Version, prepare));
				} else {
					break;
				}
			}

			if (results.Count > 0) {
				//We got at least one event in the correct age range, so we will return whatever was valid and indicate where to read from next
				nextEventNumber = results[0].EventNumber + 1;
				results.Reverse();

				var isEndOfStream = endEventNumber >= lastEventNumber;
				return new IndexReadStreamResult(endEventNumber, maxCount, results.ToArray(), metadata,
					nextEventNumber, lastEventNumber, isEndOfStream);
			}

			//we didn't find anything valid yet, now we need to search
			//the entries we found were all either scavenged, or expired, or for another stream.

			//check high value will be valid, otherwise early return.
			// this resolves hash collisions itself
			var lastEvent = ReadPrepareInternal(reader, streamId, eventNumber: lastEventNumber);
			if (lastEvent == null || lastEvent.TimeStamp < ageThreshold || lastEventNumber < fromEventNumber) {
				//No events in the stream are < max age, so return an empty set
				return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
					metadata, lastEventNumber + 1, lastEventNumber, isEndOfStream: true);
			}

			var low = indexEntries[0].Version;
			var high = lastEventNumber;
			while (low <= high) {
				var mid = low + ((high - low) / 2);
				indexEntries = tableIndex.GetRange(streamId, mid, mid + maxCount - 1);
				if (indexEntries.Count > 0) {
					nextEventNumber = indexEntries[0].Version + 1;
				}

				// be really careful if adjusting these, to make sure that the loop still terminates
				var (lowPrepareVersion, lowPrepare) = LowPrepare(reader, indexEntries, streamId);
				if (lowPrepare?.TimeStamp >= ageThreshold) {
					high = mid - 1;
					nextEventNumber = lowPrepareVersion;
					continue;
				}

				var (highPrepareVersion, highPrepare) = HighPrepare(reader, indexEntries, streamId);
				if (highPrepare?.TimeStamp < ageThreshold) {
					low = mid + indexEntries.Count;
					continue;
				}

				//ok, some entries must match, if not (due to time moving forwards) we can just reissue based on the current mid
				for (int i = 0; i < indexEntries.Count; i++) {
					var prepare = ReadPrepareInternal(reader, indexEntries[i].Position);

					if (prepare == null || prepare.EventStreamId != streamId)
						continue;

					if (prepare?.TimeStamp >= ageThreshold) {
						results.Add(new EventRecord(indexEntries[i].Version, prepare));
					} else {
						break;
					}
				}

				if (results.Count > 0) {
					//We got at least one event in the correct age range, so we will return whatever was valid and indicate where to read from next
					endEventNumber = results[0].EventNumber;
					nextEventNumber = endEventNumber + 1;
					results.Reverse();
					var isEndOfStream = endEventNumber >= lastEventNumber;

					var maxEventNumberToReturn = fromEventNumber + maxCount - 1;
					while (results.Count > 0 && results[^1].EventNumber > maxEventNumberToReturn) {
						nextEventNumber = results[^1].EventNumber;
						results.Remove(results[^1]);
						isEndOfStream = false;
					}

					return new IndexReadStreamResult(endEventNumber, maxCount, results.ToArray(), metadata,
						nextEventNumber, lastEventNumber, isEndOfStream);
				}

				break;
			}

			//We didn't find anything, send back to the client with the latest position to retry
			return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
				metadata, nextEventNumber, lastEventNumber, isEndOfStream: false);

			[MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
			static (long, PrepareLogRecord) LowPrepare(
				TFReaderLease tfReaderLease,
				IReadOnlyList<IndexEntry> entries,
				string streamId) {

				for (int i = entries.Count - 1; i >= 0; i--) {
					var prepare = ReadPrepareInternal(tfReaderLease, entries[i].Position);
					if (prepare != null && prepare.EventStreamId == streamId)
						return (entries[i].Version, prepare);
				}

				return (default, null);
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
			static (long, PrepareLogRecord) HighPrepare(
				TFReaderLease tfReaderLease,
				IReadOnlyList<IndexEntry> entries,
				string streamId) {

				for (int i = 0; i < entries.Count; i++) {
					var prepare = ReadPrepareInternal(tfReaderLease, entries[i].Position);
					if (prepare != null && prepare.EventStreamId == streamId)
						return (entries[i].Version, prepare);
				}

				return (default, null);
			}
		}

		IndexReadStreamResult IIndexReader.
			ReadStreamEventsBackward(string streamId, long fromEventNumber, int maxCount) {
			return ReadStreamEventsBackwardInternal(streamId, fromEventNumber, maxCount, _skipIndexScanOnRead);
		}

		private IndexReadStreamResult ReadStreamEventsBackwardInternal(string streamId, long fromEventNumber,
			int maxCount, bool skipIndexScanOnRead) {
			Ensure.NotNullOrEmpty(streamId, "streamId");
			Ensure.Positive(maxCount, "maxCount");

			using (var reader = _backend.BorrowReader()) {
				var lastEventNumber = GetStreamLastEventNumberCached(reader, streamId);
				var metadata = GetStreamMetadataCached(reader, streamId);
				if (lastEventNumber == EventNumber.DeletedStream)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.StreamDeleted,
						StreamMetadata.Empty, lastEventNumber);
				if (lastEventNumber == ExpectedVersion.NoStream || metadata.TruncateBefore == EventNumber.DeletedStream)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.NoStream, metadata,
						lastEventNumber);
				if (lastEventNumber == EventNumber.Invalid)
					return new IndexReadStreamResult(fromEventNumber, maxCount, ReadStreamResult.NoStream, metadata,
						lastEventNumber);

				long endEventNumber = fromEventNumber < 0 ? lastEventNumber : fromEventNumber;
				long startEventNumber = Math.Max(0L, endEventNumber - maxCount + 1);
				bool isEndOfStream = false;

				long minEventNumber = 0;
				if (metadata.MaxCount.HasValue)
					minEventNumber = Math.Max(minEventNumber, lastEventNumber - metadata.MaxCount.Value + 1);
				if (metadata.TruncateBefore.HasValue)
					minEventNumber = Math.Max(minEventNumber, metadata.TruncateBefore.Value);
				if (endEventNumber < minEventNumber)
					return new IndexReadStreamResult(fromEventNumber, maxCount, IndexReadStreamResult.EmptyRecords,
						metadata, -1, lastEventNumber, isEndOfStream: true);

				if (startEventNumber <= minEventNumber) {
					isEndOfStream = true;
					startEventNumber = minEventNumber;
				}

				var recordsQuery = _tableIndex.GetRange(streamId, startEventNumber, endEventNumber)
					.Select(x => new {x.Version, Prepare = ReadPrepareInternal(reader, x.Position)})
					.Where(x => x.Prepare != null && x.Prepare.EventStreamId == streamId);
				if (!skipIndexScanOnRead) {
					recordsQuery = recordsQuery.OrderByDescending(x => x.Version)
						.GroupBy(x => x.Version).Select(x => x.Last());
					;
				}

				if (metadata.MaxAge.HasValue) {
					var ageThreshold = DateTime.UtcNow - metadata.MaxAge.Value;
					recordsQuery = recordsQuery.Where(x => x.Prepare.TimeStamp >= ageThreshold);
				}

				var records = recordsQuery.Select(x => new EventRecord(x.Version, x.Prepare)).ToArray();

				isEndOfStream = isEndOfStream
				                || startEventNumber == 0
				                || (startEventNumber <= lastEventNumber
				                    && (records.Length == 0 ||
				                        records[records.Length - 1].EventNumber != startEventNumber));
				long nextEventNumber = isEndOfStream ? -1 : Math.Min(startEventNumber - 1, lastEventNumber);
				return new IndexReadStreamResult(endEventNumber, maxCount, records, metadata,
					nextEventNumber, lastEventNumber, isEndOfStream);
			}
		}

		public string GetEventStreamIdByTransactionId(long transactionId) {
			Ensure.Nonnegative(transactionId, "transactionId");
			using (var reader = _backend.BorrowReader()) {
				var res = ReadPrepareInternal(reader, transactionId);
				return res == null ? null : res.EventStreamId;
			}
		}

		public StorageMessage.EffectiveAcl GetEffectiveAcl(string streamId) {
			using (var reader = _backend.BorrowReader()) {
				var sysSettings = _backend.GetSystemSettings() ?? SystemSettings.Default;
				StreamAcl acl;
				StreamAcl sysAcl;
				StreamAcl defAcl;
				var meta = GetStreamMetadataCached(reader, streamId);
				if (SystemStreams.IsSystemStream(streamId)) {
					defAcl = SystemSettings.Default.SystemStreamAcl;
					sysAcl = sysSettings.SystemStreamAcl ?? defAcl;
					acl = meta.Acl ?? sysAcl;
				} else {
					defAcl = SystemSettings.Default.UserStreamAcl;
					sysAcl = sysSettings.UserStreamAcl ?? defAcl;
					acl = meta.Acl ?? sysAcl;
				}
				return new StorageMessage.EffectiveAcl(acl, sysAcl, defAcl);
			}
		}
		
		long IIndexReader.GetStreamLastEventNumber(string streamId) {
			Ensure.NotNullOrEmpty(streamId, "streamId");
			using (var reader = _backend.BorrowReader()) {
				return GetStreamLastEventNumberCached(reader, streamId);
			}
		}

		StreamMetadata IIndexReader.GetStreamMetadata(string streamId) {
			Ensure.NotNullOrEmpty(streamId, "streamId");
			using (var reader = _backend.BorrowReader()) {
				return GetStreamMetadataCached(reader, streamId);
			}
		}

		private long GetStreamLastEventNumberCached(TFReaderLease reader, string streamId) {
			// if this is metastream -- check if original stream was deleted, if yes -- metastream is deleted as well
			if (SystemStreams.IsMetastream(streamId)
			    && GetStreamLastEventNumberCached(reader, SystemStreams.OriginalStreamOf(streamId)) ==
			    EventNumber.DeletedStream)
				return EventNumber.DeletedStream;

			var cache = _backend.TryGetStreamLastEventNumber(streamId);
			if (cache.LastEventNumber != null) {
				Interlocked.Increment(ref _cachedStreamInfo);
				return cache.LastEventNumber.GetValueOrDefault();
			}

			Interlocked.Increment(ref _notCachedStreamInfo);
			var lastEventNumber = GetStreamLastEventNumberUncached(reader, streamId);

			// Conditional update depending on previously returned cache info version.
			// If version is not correct -- nothing is changed in cache.
			// This update is conditioned to not interfere with updating stream cache info by commit procedure
			// (which is the source of truth).
			var res = _backend.UpdateStreamLastEventNumber(cache.Version, streamId, lastEventNumber);
			return res ?? lastEventNumber;
		}

		private long GetStreamLastEventNumberUncached(TFReaderLease reader, string streamId) {
			IndexEntry latestEntry;
			if (!_tableIndex.TryGetLatestEntry(streamId, out latestEntry))
				return ExpectedVersion.NoStream;

			var rec = ReadPrepareInternal(reader, latestEntry.Position);
			if (rec == null)
				throw new Exception(
					$"Could not read latest stream's prepare for stream '{streamId}' at position {latestEntry.Position}");

			int count = 0;
			long startVersion = 0;
			long latestVersion = long.MinValue;
			if (rec.EventStreamId == streamId) {
				startVersion = Math.Max(latestEntry.Version, latestEntry.Version + 1);
				latestVersion = latestEntry.Version;
			}

			foreach (var indexEntry in _tableIndex.GetRange(streamId, startVersion, long.MaxValue,
				limit: _hashCollisionReadLimit + 1)) {
				var r = ReadPrepareInternal(reader, indexEntry.Position);
				if (r != null && r.EventStreamId == streamId) {
					if (latestVersion == long.MinValue) {
						latestVersion = indexEntry.Version;
						continue;
					}

					return latestVersion < indexEntry.Version ? indexEntry.Version : latestVersion;
				}

				count++;
				Interlocked.Increment(ref _hashCollisions);
				if (count > _hashCollisionReadLimit) {
					Log.Error("A hash collision resulted in not finding the last event number for the stream {stream}.",
						streamId);
					return EventNumber.Invalid;
				}
			}

			return latestVersion == long.MinValue ? ExpectedVersion.NoStream : latestVersion;
		}

		private bool OriginalStreamExists(TFReaderLease reader, string metaStreamId) {
			if (SystemStreams.IsSystemStream(metaStreamId)) {
				var originalStreamId = SystemStreams.OriginalStreamOf(metaStreamId);
				var lastEventNumber = GetStreamLastEventNumberCached(reader, originalStreamId);
				if (lastEventNumber == ExpectedVersion.NoStream || lastEventNumber == EventNumber.DeletedStream)
					return false;
				return true;
			}

			return false;
		}

		private StreamMetadata GetStreamMetadataCached(TFReaderLease reader, string streamId) {
			// if this is metastream -- check if original stream was deleted, if yes -- metastream is deleted as well
			if (SystemStreams.IsMetastream(streamId))
				return _metastreamMetadata;

			var cache = _backend.TryGetStreamMetadata(streamId);
			if (cache.Metadata != null) {
				Interlocked.Increment(ref _cachedStreamInfo);
				return cache.Metadata;
			}

			Interlocked.Increment(ref _notCachedStreamInfo);
			var streamMetadata = GetStreamMetadataUncached(reader, streamId);

			// Conditional update depending on previously returned cache info version.
			// If version is not correct -- nothing is changed in cache.
			// This update is conditioned to not interfere with updating stream cache info by commit procedure
			// (which is the source of truth).
			var res = _backend.UpdateStreamMetadata(cache.Version, streamId, streamMetadata);
			return res ?? streamMetadata;
		}

		private StreamMetadata GetStreamMetadataUncached(TFReaderLease reader, string streamId) {
			var metastreamId = SystemStreams.MetastreamOf(streamId);
			var metaEventNumber = GetStreamLastEventNumberCached(reader, metastreamId);
			if (metaEventNumber == ExpectedVersion.NoStream || metaEventNumber == EventNumber.DeletedStream)
				return StreamMetadata.Empty;

			PrepareLogRecord prepare = ReadPrepareInternal(reader, metastreamId, metaEventNumber);
			if (prepare == null)
				throw new Exception(string.Format(
					"ReadPrepareInternal could not find metaevent #{0} on metastream '{1}'. "
					+ "That should never happen.", metaEventNumber, metastreamId));

			if (prepare.Data.Length == 0 || prepare.Flags.HasNoneOf(PrepareFlags.IsJson))
				return StreamMetadata.Empty;

			try {
				var metadata = StreamMetadata.FromJsonBytes(prepare.Data);
				if (prepare.Version == LogRecordVersion.LogRecordV0 && metadata.TruncateBefore == int.MaxValue) {
					metadata = new StreamMetadata(metadata.MaxCount, metadata.MaxAge, EventNumber.DeletedStream,
						metadata.TempStream, metadata.CacheControl, metadata.Acl);
				}

				return metadata;
			} catch (Exception) {
				return StreamMetadata.Empty;
			}
		}
	}
}
