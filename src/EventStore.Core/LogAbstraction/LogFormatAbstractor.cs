using System;
using EventStore.Common.Utils;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction.Common;
using EventStore.Core.LogV2;
using EventStore.Core.LogV3;
using EventStore.Core.LogV3.FASTER;
using EventStore.Core.Settings;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using LogV3StreamId = System.UInt32;

namespace EventStore.Core.LogAbstraction {
	public class LogFormatAbstractorOptions {
		public string IndexDirectory { get; init; }
		public bool InMemory { get; init; }
		public int InitialReaderCount { get; init; } = ESConsts.PTableInitialReaderCount;
		public int MaxReaderCount { get; init; } = 100;
		public long StreamExistenceFilterSize { get; init; }
		public ICheckpoint StreamExistenceFilterCheckpoint { get; init; }
		public Func<TFReaderLease> TFReaderLeaseFactory { get; init; }
		public IReadOnlyCheckpoint ChaserCheckpoint { get; init; }
	}

	public interface ILogFormatAbstractorFactory<TStreamId> {
		LogFormatAbstractor<TStreamId> Create(LogFormatAbstractorOptions options);
	}

	//qq important thing remaining is to check if we are populating the appropriate caches
	// when we are given the opportunity, or if we are going to have an unnecessary
	// cache miss the first time we read a stream we we just populated...
	// consider carefully what happens if we do a read just as the first prepares of a
	// stream are getting added to the index. we want it to atomically switch from
	//   1. shortcutting to a nostream to
	//   2. finding the correct value in the lru
	// without any 'uncached' lookups in v2 or v3.
	public class LogV2FormatAbstractorFactory : ILogFormatAbstractorFactory<string> {
		public LogFormatAbstractor<string> Create(LogFormatAbstractorOptions options) {
			var lowHasher = new XXHashUnsafe();
			var highHasher = new Murmur3AUnsafe();
			var longHasher = new CompositeHasher<string>(lowHasher, highHasher);
			var streamExistenceFilter = GenStreamExistenceFilter(options, longHasher);
			var streamNameIndex = new LogV2StreamNameIndex(streamExistenceFilter);

			return new LogFormatAbstractor<string>(
				lowHasher: lowHasher,
				highHasher: highHasher,
				streamNameIndex: streamNameIndex,
				streamNameIndexConfirmer: streamNameIndex,
				streamIds: streamNameIndex,
				metastreams: new LogV2SystemStreams(),
				streamNamesProvider: GenStreamNamesProvider(options, streamNameIndex),
				streamIdValidator: new LogV2StreamIdValidator(),
				emptyStreamId: string.Empty,
				streamIdSizer: new LogV2Sizer(),
				streamExistenceFilter: streamExistenceFilter,
				streamExistenceFilterReader: streamExistenceFilter,
				recordFactory: new LogV2RecordFactory(),
				supportsExplicitTransactions: true);
		}

		public static INameExistenceFilter GenStreamExistenceFilter(
			LogFormatAbstractorOptions options,
			ILongHasher<string> longHasher) {

			if (options.InMemory || options.StreamExistenceFilterSize == 0) {
				return new NoNameExistenceFilter();
			}

			var nameExistenceFilter = new StreamExistenceFilter(
				directory: $"{options.IndexDirectory}/stream-existence",
				filterName: "StreamExistenceFilter",
				size: options.StreamExistenceFilterSize,
				checkpoint: options.StreamExistenceFilterCheckpoint,
				initialReaderCount: options.InitialReaderCount,
				maxReaderCount: options.MaxReaderCount,
				checkpointInterval: TimeSpan.FromSeconds(60),
				hasher: longHasher);

			return nameExistenceFilter;
		}

		static IStreamNamesProvider<string> GenStreamNamesProvider(LogFormatAbstractorOptions options, LogV2StreamNameIndex streamNameIndex) =>
			new AdHocStreamNamesProvider<string>(
				init: () => (new LogV2SystemStreams(), streamNameIndex, null),
				setReader: _ => (null, null, null),
				setTableIndex: tableIndex => new LogV2StreamExistenceFilterInitializer(
					options.TFReaderLeaseFactory, options.ChaserCheckpoint, tableIndex));
	}

	public class LogV3FormatAbstractorFactory : ILogFormatAbstractorFactory<LogV3StreamId> {
		public LogFormatAbstractor<LogV3StreamId> Create(LogFormatAbstractorOptions options) {
			var metastreams = new LogV3Metastreams();
			var streamNamesProvider = GenStreamNamesProvider(metastreams);

			var streamNameIndexPersistence = GenStreamNameIndexPersistence(options);
			var streamExistenceFilter = GenStreamExistenceFilter(options);
			var streamNameIndex = GenStreamNameIndex(
				options, streamExistenceFilter,
				streamNameIndexPersistence, metastreams);

			var streamIds = streamNameIndexPersistence
				.Wrap(x => new NameExistenceFilterValueLookupDecorator<LogV3StreamId>(x, streamExistenceFilter))
				.Wrap(x => new StreamIdLookupMetastreamDecorator(x, metastreams));

			var abstractor = new LogFormatAbstractor<LogV3StreamId>(
				lowHasher: new IdentityLowHasher(),
				highHasher: new IdentityHighHasher(),
				streamNameIndex: new StreamNameIndexMetastreamDecorator(streamNameIndex, metastreams),
				streamNameIndexConfirmer: streamNameIndex,
				streamIds: streamIds,
				metastreams: metastreams,
				streamNamesProvider: streamNamesProvider,
				streamIdValidator: new LogV3StreamIdValidator(),
				emptyStreamId: 0,
				streamIdSizer: new LogV3Sizer(),
				streamExistenceFilter: streamExistenceFilter,
				// this is for the indexreader to check the existence of the stream using a streamId
				// its important for LogV2 (because it has no stream name index)
				// but not applicable to LogV3 (because you need a streamName not id to check the filter)
				streamExistenceFilterReader: new NoExistenceFilterReader(),
				recordFactory: new LogV3RecordFactory(),
				supportsExplicitTransactions: false);
			return abstractor;
		}

		static IStreamNamesProvider<LogV3StreamId> GenStreamNamesProvider(IMetastreamLookup<LogV3StreamId> metastreams) {
			return new AdHocStreamNamesProvider<LogV3StreamId>(
				init: () => (null, null, null),
				setReader: indexReader => {
					INameLookup<LogV3StreamId> streamNames = new StreamIdToNameFromStandardIndex(indexReader);
					var systemStreams = new LogV3SystemStreams(metastreams, streamNames);
					streamNames = new StreamNameLookupMetastreamDecorator(streamNames, metastreams);
					//qq consider if the enumerator should use the metastream decorated one
					var streamExistenceFilterInitializer = new LogV3StreamExistenceFilterInitializer(streamNames);
					return (systemStreams, streamNames, streamExistenceFilterInitializer);
				},
				setTableIndex: _ => null);
		}

		static NameIndex GenStreamNameIndex(
			LogFormatAbstractorOptions options,
			INameExistenceFilter existenceFilter,
			INameIndexPersistence<LogV3StreamId> persistence,
			IMetastreamLookup<LogV3StreamId> metastreams) {

			var streamNameIndex = new NameIndex(
				indexName: "StreamNameIndex",
				firstValue: LogV3SystemStreams.FirstRealStream,
				valueInterval: LogV3SystemStreams.StreamInterval,
				existenceFilter: existenceFilter,
				persistence: persistence,
				metastreams: metastreams);
			return streamNameIndex;
		}

		static INameIndexPersistence<LogV3StreamId> GenStreamNameIndexPersistence(LogFormatAbstractorOptions options) {
			if (options.InMemory) {
				return new NameIndexInMemoryPersistence();
			}

			var persistence = new FASTERNameIndexPersistence(
				indexName: "StreamNameIndexPersistence",
				logDir: $"{options.IndexDirectory}/stream-name-index",
				firstValue: LogV3SystemStreams.FirstRealStream,
				valueInterval: LogV3SystemStreams.StreamInterval,
				initialReaderCount: options.InitialReaderCount,
				maxReaderCount: options.MaxReaderCount,
				enableReadCache: false,
				checkpointInterval: TimeSpan.FromSeconds(60));
			return persistence;
		}

		public static INameExistenceFilter GenStreamExistenceFilter(LogFormatAbstractorOptions options) {
			if (options.InMemory || options.StreamExistenceFilterSize == 0) {
				return new NoNameExistenceFilter();
			}

			var nameExistenceFilter = new StreamExistenceFilter(
				directory: $"{options.IndexDirectory}/stream-existence",
				filterName: "StreamExistenceFilter",
				size: options.StreamExistenceFilterSize,
				checkpoint: options.StreamExistenceFilterCheckpoint,
				initialReaderCount: options.InitialReaderCount,
				maxReaderCount: options.MaxReaderCount,
				checkpointInterval: TimeSpan.FromSeconds(60),
				hasher: null)
			.Wrap(x => new StreamExistenceFilterValidator(x));

			return nameExistenceFilter;
		}
	}

	public class LogFormatAbstractor<TStreamId> : IDisposable {
		public LogFormatAbstractor(
			IHasher<TStreamId> lowHasher,
			IHasher<TStreamId> highHasher,
			INameIndex<TStreamId> streamNameIndex,
			INameIndexConfirmer<TStreamId> streamNameIndexConfirmer,
			IValueLookup<TStreamId> streamIds,
			IMetastreamLookup<TStreamId> metastreams,
			IStreamNamesProvider<TStreamId> streamNamesProvider,
			IValidator<TStreamId> streamIdValidator,
			TStreamId emptyStreamId,
			ISizer<TStreamId> streamIdSizer,
			INameExistenceFilter streamExistenceFilter,
			IExistenceFilterReader<TStreamId> streamExistenceFilterReader,
			IRecordFactory<TStreamId> recordFactory,
			bool supportsExplicitTransactions) {

			LowHasher = lowHasher;
			HighHasher = highHasher;
			StreamNameIndex = streamNameIndex;
			StreamNameIndexConfirmer = streamNameIndexConfirmer;
			StreamIds = streamIds;
			Metastreams = metastreams;
			StreamNamesProvider = streamNamesProvider;
			StreamIdValidator = streamIdValidator;
			EmptyStreamId = emptyStreamId;
			StreamIdSizer = streamIdSizer;
			StreamExistenceFilter = streamExistenceFilter;
			StreamExistenceFilterReader = streamExistenceFilterReader;

			RecordFactory = recordFactory;
			SupportsExplicitTransactions = supportsExplicitTransactions;
		}

		public void Dispose() {
			StreamNameIndexConfirmer?.Dispose();
			StreamExistenceFilter?.Dispose();
		}

		public IHasher<TStreamId> LowHasher { get; }
		public IHasher<TStreamId> HighHasher { get; }
		public INameIndex<TStreamId> StreamNameIndex { get; }
		public INameIndexConfirmer<TStreamId> StreamNameIndexConfirmer { get; }
		public IValueLookup<TStreamId> StreamIds { get; }
		public IMetastreamLookup<TStreamId> Metastreams { get; }
		public IStreamNamesProvider<TStreamId> StreamNamesProvider { get; }
		public IValidator<TStreamId> StreamIdValidator { get; }
		public TStreamId EmptyStreamId { get; }
		public ISizer<TStreamId> StreamIdSizer { get; }
		public INameExistenceFilter StreamExistenceFilter { get; }
		public IExistenceFilterReader<TStreamId> StreamExistenceFilterReader { get; }
		public IRecordFactory<TStreamId> RecordFactory { get; }

		public INameLookup<TStreamId> StreamNames => StreamNamesProvider.StreamNames;
		public ISystemStreamLookup<TStreamId> SystemStreams => StreamNamesProvider.SystemStreams;
		public INameExistenceFilterInitializer StreamExistenceFilterInitializer => StreamNamesProvider.StreamExistenceFilterInitializer;
		public bool SupportsExplicitTransactions { get; }
	}
}
