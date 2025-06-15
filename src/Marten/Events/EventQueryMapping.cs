using System;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Marten.Linq.Parsing;
using Marten.Linq.SqlGeneration.Filters;
using Marten.Schema;
using Weasel.Core;
using Weasel.Postgresql;

namespace Marten.Events;

public class EventQueryMapping: DocumentMapping
{
    public EventQueryMapping(StoreOptions storeOptions): base(typeof(IEvent), storeOptions)
    {
        DatabaseSchemaName = storeOptions.Events.DatabaseSchemaName;

        TenancyStyle = storeOptions.Events.TenancyStyle;

        TableName = new PostgresqlObjectName(DatabaseSchemaName, "mt_events");

        registerQueryableMember(static x => x.Id, "id");
        registerQueryableMember(static x => x.Sequence, "seq_id");
        if (storeOptions.Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            registerQueryableMember(static x => x.StreamId, "stream_id");
        }
        else
        {
            registerQueryableMember(static x => x.StreamKey, "stream_id");
        }

        registerQueryableMember(static x => x.Version, "version");
        registerQueryableMember(static x => x.Timestamp, "timestamp");

        // Is archived needs to be a little different
        var member = ReflectionHelper.GetProperty<IEvent>(static x => x.IsArchived);
        QueryMembers.ReplaceMember(member, new IsArchivedMember());

        registerQueryableMember(static x => x.EventTypeName, "type");
        registerQueryableMember(static x => x.DotNetTypeName, SchemaConstants.DotNetTypeColumn);


        if (storeOptions.EventGraph.Metadata.CorrelationId.Enabled)
        {
            registerQueryableMember(static x => x.CorrelationId, storeOptions.EventGraph.Metadata.CorrelationId.Name);
        }

        if (storeOptions.EventGraph.Metadata.CausationId.Enabled)
        {
            registerQueryableMember(static x => x.CausationId, storeOptions.EventGraph.Metadata.CausationId.Name);
        }
    }

    public override DbObjectName TableName { get; }

    private void registerQueryableMember(Expression<Func<IEvent, object>> property, string columnName)
    {
        var member = ReflectionHelper.GetProperty(property);

        var field = DuplicateField(new MemberInfo[] { member }, columnName: columnName);
        QueryMembers.ReplaceMember(member, field);
    }
}
