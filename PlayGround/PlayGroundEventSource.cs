using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;

namespace PlayGround
{
    [EventSource(Name = "PlayGround")]
    public class PlayGroundEventSource : EventSource
    {
        private static readonly Lazy<PlayGroundEventSource> Instance =
            new Lazy<PlayGroundEventSource>(() => new PlayGroundEventSource());

        private PlayGroundEventSource()
        {
        }

        public static PlayGroundEventSource Log
        {
            get { return Instance.Value; }
        }

        [Event(1, Level = EventLevel.Informational, Keywords = Keywords.Trace, Message = "{0}")]
        public void Trace(string msg, ulong UserId, string MachineName)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(1, msg, UserId, MachineName);
            }
        }

        /// <summary>
        /// Custom defined event keywords.
        /// </summary>
        public static class Keywords
        {
            /// <summary>
            /// Keyword for sink.
            /// </summary>
            public const EventKeywords Trace = (EventKeywords)0x1;
        }
    }
}
