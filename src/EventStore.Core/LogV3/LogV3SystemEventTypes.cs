using System;
using EventStore.Core.Services;

namespace EventStore.Core.LogV3
{
	public class LogV3SystemEventTypes {

		public const UInt32 FirstRealEventType = 1024;
		public const UInt32 EventTypeInterval = 1;

		public const UInt32 EmptyEventType = 0;
		public const UInt32 StreamDeleted = 1;
		public const UInt32 StatsCollection = 2;
		public const UInt32 LinkTo = 3;
		public const UInt32 StreamReference = 4;
		public const UInt32 StreamMetadata = 5;
		public const UInt32 Settings = 6;
		public const UInt32 StreamCreated = 7;

		public const UInt32 V2__StreamCreated_InIndex = 8;
		public const UInt32 V1__StreamCreated__ = 9;
		public const UInt32 V1__StreamCreatedImplicit__ = 10;

		public const UInt32 ScavengeStarted = 11;
		public const UInt32 ScavengeCompleted = 12;
		public const UInt32 ScavengeChunksCompleted = 13;
		public const UInt32 ScavengeMergeCompleted = 14;
		public const UInt32 ScavengeIndexCompleted = 15;

		public const UInt32 EventDefined = 16;
		
		public static bool TryGetSystemEventTypeId(string type, out UInt32 eventTypeId) {
			switch (type) {
				case SystemEventTypes.EmptyEventType:
					eventTypeId = EmptyEventType;
					return true;
				case SystemEventTypes.EventDefined:
					eventTypeId = EventDefined;
					return true;
				case SystemEventTypes.StreamCreated:
					eventTypeId = StreamCreated;
					return true;
				case SystemEventTypes.StreamDeleted:
					eventTypeId = StreamDeleted;
					return true;
				case SystemEventTypes.StreamMetadata:
					eventTypeId = StreamMetadata;
					return true;
//qq implement other types ??
				default:
					eventTypeId = 0;
					return false;
			}
		}
		
		public static bool TryGetVirtualEventType(UInt32 eventTypeId, out string name) {
			if (!IsVirtualEventType(eventTypeId)) {
				name = null;
				return false;
			}

			name = eventTypeId switch {
				EmptyEventType => SystemEventTypes.EmptyEventType,
				StreamMetadata => SystemEventTypes.StreamMetadata,
				StreamCreated => SystemEventTypes.StreamCreated,
				StreamDeleted => SystemEventTypes.StreamDeleted,
				_ => null,
			};

			return name != null;
		}
		
		private static bool IsVirtualEventType(UInt32 eventTypeId) => eventTypeId < FirstRealEventType;
	}
}
