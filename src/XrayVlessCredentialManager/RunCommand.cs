using CliFx;
using CliFx.Binding;
using CliFx.Infrastructure;

namespace XrayVlessCredentialManager;

[Command("run")]
public sealed partial class RunCommand : ICommand
{
    [CommandOption("xray-api")]
    public required string XrayApi { get; set; }

    [CommandOption("inbound-tag")]
    public required string InboundTag { get; set; }

    [CommandOption("credential-manager-port")]
    public required int CredentialManagerPort { get; set; }

    [CommandOption("credential-database")]
    public required FileInfo CredentialDatabase { get; set; }

    public async ValueTask ExecuteAsync(IConsole console)
    {
        var cancellationToken = console.RegisterCancellationHandler();

        await using var credentials = await XrayVlessCredentialManager.OpenAsync(
            XrayApi, InboundTag, CredentialDatabase.FullName, cancellationToken);

        await CredentialManageServer.RunAsync(credentials, CredentialManagerPort);
    }
}
