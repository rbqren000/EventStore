﻿using System;
using System.Collections.Generic;
using EventStore.Core.Index;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.LogCommon;
using StreamId = System.String;

namespace EventStore.Core.LogV2 {
	/// <summary>
	/// Stream name existence filter initializer for Log V2
	/// Reads the index and transaction log to populate the stream name existence filter from the last checkpoint.
	/// May add a stream hash more than once.
	/// </summary>
	public class LogV2StreamNameExistenceFilterInitializer : INameExistenceFilterInitializer {
		private readonly Func<TFReaderLease> _tfReaderFactory;
		private readonly IReadOnlyCheckpoint _chaserCheckpoint;
		private ITableIndex _tableIndex;

		public LogV2StreamNameExistenceFilterInitializer(
			Func<TFReaderLease> tfReaderFactory,
			IReadOnlyCheckpoint chaserCheckpoint) {
			_tfReaderFactory = tfReaderFactory;
			_chaserCheckpoint = chaserCheckpoint;
		}

		public void SetTableIndex(ITableIndex tableIndex) {
			_tableIndex = tableIndex;
		}

		//qq wanna split this into two methods: EnumerateStreamsInIndex and EnumerateStreamsInLog and call them both from initilize
		// i think that'll make it neater
		private IEnumerable<(ulong streamHash, string streamName, long checkpoint)> EnumerateStreamHashes(long lastCheckpoint) {
			if (_tableIndex == null) throw new Exception("Call SetTableIndex first");

			using var reader = _tfReaderFactory();
			var buildToPosition = _chaserCheckpoint.Read();

			if (lastCheckpoint == -1L) { // if we do not have a checkpoint, rebuild the list of stream hashes from the index
				ulong previousHash = ulong.MaxValue;
				foreach (var entry in _tableIndex.IterateAll()) {
					if (entry.Stream == previousHash) {
						continue;
					}
					previousHash = entry.Stream;
					yield return (previousHash, null, -1L);
				}
				if (previousHash != ulong.MaxValue) { // send a checkpoint with the last stream hash
					yield return (previousHash, null, Math.Max(_tableIndex.PrepareCheckpoint, _tableIndex.CommitCheckpoint));
				}
				reader.Reposition(Math.Max(_tableIndex.PrepareCheckpoint, _tableIndex.CommitCheckpoint));
			} else {
				reader.Reposition(lastCheckpoint);
			}

			while (true) {
				if (!TryReadNextLogRecord(reader, buildToPosition, out var record, out var postPosition)) {
					break;
				}
				switch (record.RecordType) {
					case LogRecordType.Prepare:
						var prepare = (IPrepareLogRecord<StreamId>) record;
						if (prepare.Flags.HasFlag(PrepareFlags.IsCommitted)) {
							yield return (0, prepare.EventStreamId, postPosition);
						}
						break;
					case LogRecordType.Commit:
						var commit = (CommitLogRecord)record;
						reader.Reposition(commit.TransactionPosition);
						if (TryReadNextLogRecord(reader, buildToPosition, out var transactionRecord, out _)) {
							var transactionPrepare = (IPrepareLogRecord<StreamId>) transactionRecord;
							yield return (0, transactionPrepare.EventStreamId, postPosition);
						} else {
							// nothing to do - may have been scavenged
						}
						reader.Reposition(postPosition);
						break;
				}

			}
		}

		public static bool TryReadNextLogRecord(TFReaderLease reader, long maxPosition, out ILogRecord record, out long postPosition) {
			var result = reader.TryReadNext();
			if (!result.Success || result.LogRecord.LogPosition >= maxPosition) {
				record = null;
				postPosition = 0L;
				return false;
			}
			record = result.LogRecord;
			postPosition = result.RecordPostPosition;
			return true;
		}

		public void Initialize(INameExistenceFilter filter) {
			var lastCheckpoint = filter.CurrentCheckpoint;
			foreach (var (hash, name, checkpoint) in EnumerateStreamHashes(lastCheckpoint)) {
				if (name == null)
					filter.Add(hash, checkpoint);
				else 
					filter.Add(name, checkpoint);
			}
		}
	}
}
