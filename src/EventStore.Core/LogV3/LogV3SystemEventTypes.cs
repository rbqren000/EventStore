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
				case SystemEventTypes.StreamDeleted:
					eventTypeId = StreamDeleted;
					return true;
				case SystemEventTypes.StatsCollection:
					eventTypeId = StatsCollection;
					return true;
				case SystemEventTypes.LinkTo:
					eventTypeId = LinkTo;
					return true;
				case SystemEventTypes.StreamReference:
					eventTypeId = StreamReference;
					return true;
				case SystemEventTypes.StreamMetadata:
					eventTypeId = StreamMetadata;
					return true;
				case SystemEventTypes.Settings:
					eventTypeId = Settings;
					return true;
				case SystemEventTypes.StreamCreated:
					eventTypeId = StreamCreated;
					return true;
				case SystemEventTypes.V2__StreamCreated_InIndex:
					eventTypeId = V2__StreamCreated_InIndex;
					return true;
				case SystemEventTypes.V1__StreamCreated__:
					eventTypeId = V1__StreamCreated__;
					return true;
				case SystemEventTypes.V1__StreamCreatedImplicit__:
					eventTypeId = V1__StreamCreatedImplicit__;
					return true;
				case SystemEventTypes.ScavengeStarted:
					eventTypeId = ScavengeStarted;
					return true;
				case SystemEventTypes.ScavengeCompleted:
					eventTypeId = ScavengeCompleted;
					return true;
				case SystemEventTypes.ScavengeChunksCompleted:
					eventTypeId = ScavengeChunksCompleted;
					return true;
				case SystemEventTypes.ScavengeMergeCompleted:
					eventTypeId = ScavengeMergeCompleted;
					return true;
				case SystemEventTypes.ScavengeIndexCompleted:
					eventTypeId = ScavengeIndexCompleted;
					return true;
				case SystemEventTypes.EventDefined:
					eventTypeId = EventDefined;
					return true;
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
				StreamDeleted => SystemEventTypes.StreamDeleted,
				StatsCollection => SystemEventTypes.StatsCollection,
				LinkTo => SystemEventTypes.LinkTo,
				StreamReference => SystemEventTypes.StreamReference,
				StreamMetadata => SystemEventTypes.StreamMetadata,
				Settings => SystemEventTypes.Settings,
				StreamCreated => SystemEventTypes.StreamCreated,
				V2__StreamCreated_InIndex => SystemEventTypes.V2__StreamCreated_InIndex,
				V1__StreamCreated__ => SystemEventTypes.V1__StreamCreated__,
				V1__StreamCreatedImplicit__ => SystemEventTypes.V1__StreamCreatedImplicit__,
				ScavengeStarted => SystemEventTypes.ScavengeStarted,
				ScavengeCompleted => SystemEventTypes.ScavengeCompleted,
				ScavengeChunksCompleted => SystemEventTypes.ScavengeChunksCompleted,
				ScavengeMergeCompleted => SystemEventTypes.ScavengeMergeCompleted,
				ScavengeIndexCompleted => SystemEventTypes.ScavengeIndexCompleted,
				EventDefined => SystemEventTypes.EventDefined,
				_ => null,
			};

			return name != null;
		}
		
		private static bool IsVirtualEventType(EventTypeId eventTypeId) => eventTypeId < FirstRealEventType;
	}
}
