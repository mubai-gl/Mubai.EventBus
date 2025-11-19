using System;

namespace Mubai.EventBus.Events
{
    /// <summary>
    /// Specify the serialized name of the event to allow different modules to align the same event contract by name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class EventNameAttribute : Attribute
    {
        public EventNameAttribute(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public string Name { get; }
    }
}
