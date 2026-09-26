using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using LiteDB;
using Xray.App.Proxyman.Command;
using Xray.Common.Protocol;
using Xray.Common.Serial;
using Xray.Proxy.Vless;

namespace XrayVlessCredentialManager;

public sealed class XrayVlessCredentialManager : IAsyncDisposable
{
    private readonly GrpcChannel channel;
    private readonly HandlerService.HandlerServiceClient xray;
    private readonly string inboundTag;
    private readonly LiteDatabase database;
    private readonly ILiteCollection<CredentialEntry> entries;

    private sealed class CredentialEntry
    {
        [BsonId]
        public Guid CredentialId { get; set; }

        public required string Credential { get; set; }

        public DateTimeOffset? Expire { get; set; }
    }

    private XrayVlessCredentialManager(
        GrpcChannel channel,
        HandlerService.HandlerServiceClient xray,
        string inboundTag,
        LiteDatabase database)
    {
        this.channel = channel;
        this.xray = xray;
        this.inboundTag = inboundTag;
        this.database = database;
        this.entries = database.GetCollection<CredentialEntry>("credentials");
    }

    public static async Task<XrayVlessCredentialManager> OpenAsync(
        string xrayApi, string inboundTag, string filePath, CancellationToken cancellationToken = default)
    {
        var channel = GrpcChannel.ForAddress(xrayApi);
        var xray = new HandlerService.HandlerServiceClient(channel);
        var database = new LiteDatabase(filePath);

        var manager = new XrayVlessCredentialManager(channel, xray, inboundTag, database);
        await manager.ResyncAsync(cancellationToken);
        return manager;
    }

    private async Task ResyncAsync(CancellationToken cancellationToken)
    {
        var response = await this.xray.GetInboundUsersAsync(
            new GetInboundUserRequest { Tag = this.inboundTag },
            cancellationToken: cancellationToken).ResponseAsync;
        var existing = response.Users
            .Select(x => x.Email)
            .ToHashSet();

        foreach (var entry in this.entries.FindAll())
        {
            if (entry.Expire is { } expire && expire <= DateTimeOffset.UtcNow)
            {
                await this.ExpireAsync(entry.CredentialId);
                continue;
            }

            var email = entry.CredentialId.ToString();
            if (!existing.Contains(email))
                await this.AddUserAsync(email, entry.Credential, cancellationToken);

            this.ScheduleExpiry(entry.CredentialId, entry.Expire);
        }
    }

    private Task AddUserAsync(string email, string uuid, CancellationToken cancellationToken) =>
        this.xray.AddUserAsync(new AddUserRequest
        {
            User = new User
            {
                Email = email,
                Account = new TypedMessage
                {
                    Type = Account.Descriptor.FullName,
                    Value = new Account { Id = uuid }.ToByteString(),
                },
            },
            InboundTag = this.inboundTag,
        }, cancellationToken: cancellationToken).ResponseAsync;

    private Task RemoveUserAsync(string email, CancellationToken cancellationToken) =>
        this.xray.RemoveUserAsync(new RemoveUserRequest
        {
            Email = email,
            InboundTag = this.inboundTag,
        }, cancellationToken: cancellationToken).ResponseAsync;

    private void ScheduleExpiry(Guid id, DateTimeOffset? expire)
    {
        if (expire is not { } e)
            return;

        _ = this.ExpireAfterAsync(id, e);
    }

    private async Task ExpireAfterAsync(Guid id, DateTimeOffset expire)
    {
        while (true)
        {
            var remaining = expire - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            await Task.Delay(remaining);
        }

        if (this.entries.FindById(id) is null)
            return;

        await this.ExpireAsync(id);
    }

    private async Task ExpireAsync(Guid id)
    {
        try
        {
            await this.RemoveUserAsync(id.ToString(), CancellationToken.None);
        }
        catch (RpcException)
        {
        }

        this.entries.Delete(id);
    }

    public async Task<(string CredentialId, string Credential)> AddAsync(
        DateTimeOffset? expire, CancellationToken cancellationToken = default)
    {
        var credentialId = Guid.NewGuid();
        var uuid = Guid.NewGuid().ToString();

        await this.AddUserAsync(credentialId.ToString(), uuid, cancellationToken);

        this.entries.Insert(new CredentialEntry
        {
            CredentialId = credentialId,
            Credential = uuid,
            Expire = expire,
        });

        this.ScheduleExpiry(credentialId, expire);

        return (credentialId.ToString(), uuid);
    }

    public (bool Exists, DateTimeOffset? Expire) Query(string credentialId)
    {
        if (!Guid.TryParse(credentialId, out var id))
            return (false, null);

        var entry = this.entries.FindById(id);
        if (entry is null)
            return (false, null);

        if (entry.Expire is { } expire && expire <= DateTimeOffset.UtcNow)
        {
            _ = this.ExpireAsync(id);
            return (false, null);
        }

        return (true, entry.Expire);
    }

    public async Task RemoveAsync(string credentialId, CancellationToken cancellationToken = default)
    {
        if (!Guid.TryParse(credentialId, out var id))
            return;

        if (this.entries.FindById(id) is null)
            return;

        await this.RemoveUserAsync(id.ToString(), cancellationToken);
        this.entries.Delete(id);
    }

    public async ValueTask DisposeAsync()
    {
        this.database.Dispose();
        this.channel.Dispose();
    }
}
