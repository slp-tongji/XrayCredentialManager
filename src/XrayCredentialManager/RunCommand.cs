using CliFx;
using CliFx.Binding;
using CliFx.Infrastructure;

namespace XrayCredentialManager;

[Command("run")]
public sealed partial class RunCommand : ICommand
{
    [CommandOption("xray-api", Description = "xray gRPC API 地址，例如 http://127.0.0.1:10085")]
    public required string XrayApi { get; set; }

    [CommandOption("inbound-tag", Description = "要管理用户的 VLESS 入站 tag")]
    public required string InboundTag { get; set; }

    [CommandOption("credential-manager-port", Description = "凭证管理 HTTP API 端口")]
    public required int CredentialManagerPort { get; set; }

    [CommandOption("credential-database", Description = "凭证数据库文件路径")]
    public required FileInfo CredentialDatabase { get; set; }

    public async ValueTask ExecuteAsync(IConsole console)
    {
        await using var credentials = await XrayCredentialManager.OpenAsync(
            XrayApi, InboundTag, CredentialDatabase.FullName);

        await console.Output.WriteLineAsync($"xray API: {XrayApi} (inbound: {InboundTag})");
        await console.Output.WriteLineAsync($"Credential manager API listening on 127.0.0.1:{CredentialManagerPort}");

        await CredentialManageServer.RunAsync(credentials, CredentialManagerPort);
    }
}
