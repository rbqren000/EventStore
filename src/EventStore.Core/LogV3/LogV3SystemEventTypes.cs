using EventStore.Core.Services;
using EventTypeId = System.UInt32;

namespace EventStore.Core.LogV3
{
	public class LogV3SystemEventTypes {

		public const EventTypeId FirstRealEventType = 1024;
		public const EventTypeId EventTypeInterval = 1;

		public const EventTypeId EmptyEventType = 0;
		public const EventTypeId StreamDeleted = 1;
		public const EventTypeId StatsCollection = 2;
		public const EventTypeId LinkTo = 3;
		public const EventTypeId StreamReference = 4;
		public const EventTypeId StreamMetadata = 5;
		public const EventTypeId Settings = 6;
		public const EventTypeId StreamCreated = 7;

		public const EventTypeId V2__StreamCreated_InIndex = 8;
		public const EventTypeId V1__StreamCreated__ = 9;
		public const EventTypeId V1__StreamCreatedImplicit__ = 10;

		public const EventTypeId ScavengeStarted = 11;
		public const EventTypeId ScavengeCompleted = 12;
		public const EventTypeId ScavengeChunksCompleted = 13;
		public const EventTypeId ScavengeMergeCompleted = 14;
		public const EventTypeId ScavengeIndexCompleted = 15;

		public const EventTypeId EventDefined = 16;
		
		public static bool TryGetSystemEventTypeId(string type, out EventTypeId eventTypeId) {
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
					eventTypeId = EmptyEventType;
					return false;
			}
		}
		
		public static bool TryGetVirtualEventType(EventTypeId eventTypeId, out string name) {
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
		
		private static bool IsVirtualEventType(EventTypeId eventTypeId) => eventTypeId < FirstRealEventType;
	}
}
