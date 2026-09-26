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
    private readonly CancellationTokenSource lifetime = new();
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(30);

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
        string xrayApi, string inboundTag, string filePath, CancellationToken cancellationToken)
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
            var email = entry.CredentialId.ToString();
            if (!existing.Contains(email))
                await this.AddUserAsync(email, entry.Credential, cancellationToken);

            this.ScheduleExpiry(entry.CredentialId, entry.Expire);
        }
    }

    private async Task AddUserAsync(string email, string uuid, CancellationToken cancellationToken)
    {
        await this.xray.AlterInboundAsync(new AlterInboundRequest
        {
            Tag = this.inboundTag,
            Operation = new TypedMessage
            {
                Type = AddUserOperation.Descriptor.FullName,
                Value = new AddUserOperation
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
                }.ToByteString(),
            },
        }, cancellationToken: cancellationToken).ResponseAsync;
    }

    private async Task RemoveUserAsync(string email, CancellationToken cancellationToken)
    {
        await this.xray.AlterInboundAsync(new AlterInboundRequest
        {
            Tag = this.inboundTag,
            Operation = new TypedMessage
            {
                Type = RemoveUserOperation.Descriptor.FullName,
                Value = new RemoveUserOperation { Email = email }.ToByteString(),
            },
        }, cancellationToken: cancellationToken).ResponseAsync;
    }

    private void ScheduleExpiry(Guid id, DateTimeOffset? expire)
    {
        if (expire is not { } e)
            return;

        _ = this.ExpireAfterAsync(id, e, this.lifetime.Token);
    }

    private async Task ExpireAfterAsync(Guid id, DateTimeOffset expire, CancellationToken cancellationToken)
    {
        while (true)
        {
            var remaining = expire - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            await Task.Delay(remaining, cancellationToken);
        }

        while (true)
        {
            if (this.entries.FindById(id) is null)
                return;

            try
            {
                await this.RemoveUserAsync(id.ToString(), cancellationToken);
            }
            catch (RpcException)
            {
                try
                {
                    if (!await this.UserExistsInXrayAsync(id.ToString(), cancellationToken))
                    {
                        this.entries.Delete(id);
                        return;
                    }
                }
                catch (RpcException)
                {
                }

                await Task.Delay(RetryDelay, cancellationToken);
                continue;
            }

            this.entries.Delete(id);
            return;
        }
    }

    private async Task<bool> UserExistsInXrayAsync(string email, CancellationToken cancellationToken)
    {
        var response = await this.xray.GetInboundUsersAsync(
            new GetInboundUserRequest { Tag = this.inboundTag },
            cancellationToken: cancellationToken).ResponseAsync;

        return response.Users.Any(x => x.Email == email);
    }

    public async Task<(string CredentialId, string Credential)> AddAsync(
        DateTimeOffset? expire, CancellationToken cancellationToken)
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

        return (true, entry.Expire);
    }

    public async Task RemoveAsync(string credentialId, CancellationToken cancellationToken)
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
        this.lifetime.Cancel();
        this.database.Dispose();
        this.channel.Dispose();
        this.lifetime.Dispose();
    }
}
