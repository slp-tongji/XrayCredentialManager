using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Tjslp.CredentialManager.Protocol;

namespace XrayVlessCredentialManager;

public sealed class CredentialManageServer : IAsyncDisposable
{
    private readonly XrayVlessCredentialManager credentials;
    private readonly WebApplication app;

    private CredentialManageServer(XrayVlessCredentialManager credentials, WebApplication app)
    {
        this.credentials = credentials;
        this.app = app;
    }

    public static async Task RunAsync(
        XrayVlessCredentialManager credentials, int port)
    {
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());

        builder.Services.AddRouting();
        builder.WebHost.UseKestrel(kestrel => kestrel.ListenLocalhost(port));

        var app = builder.Build();

        await using var server = new CredentialManageServer(credentials, app);

        app.MapPost("/create", server.HandleCreate);
        app.MapPost("/query", server.HandleQuery);
        app.MapPost("/revoke", server.HandleRevoke);

        await app.RunAsync();
    }

    private async Task<CreateResponse> HandleCreate(CreateRequest request)
    {
        var (credentialId, credential) = await this.credentials.AddAsync(request.Expire);
        return new CreateResponse(credentialId, credential, request.Expire);
    }

    private QueryResponse HandleQuery(QueryRequest request)
    {
        var items = new List<QueryItem>();
        foreach (var credentialId in request.CredentialIds)
        {
            var (exists, expire) = this.credentials.Query(credentialId);
            if (exists)
                items.Add(new QueryItem(credentialId, expire));
        }

        return new QueryResponse(items);
    }

    private async Task<RevokeResponse> HandleRevoke(RevokeRequest request)
    {
        await this.credentials.RemoveAsync(request.CredentialId);
        return new RevokeResponse(true);
    }

    public async ValueTask DisposeAsync()
    {
        await this.app.DisposeAsync();
    }
}
