using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.SignalR.Client;

// namespace ...;

public abstract class HubComponentBase : ComponentBase, IAsyncDisposable
{
    [Inject] 
    private NavigationManager Navigation { get; set; }
    
    protected HubConnection Hub { get; private set; }
    
    protected bool IsHubConnected => Hub != null && Hub.State == HubConnectionState.Connected;



    protected override async Task OnInitializedAsync()
    {
        Hub ??= new HubConnectionBuilder()
            .WithUrl(Navigation.ToAbsoluteUri(Hubs.DraftHub.HubUrl))
            .WithAutomaticReconnect()
            .Build();

        if (!IsHubConnected)
        {
            try
            {
                await Hub.StartAsync();
            }
            catch { }
        }

        if (IsHubConnected)
        {
            try
            {
                AddHubHandlers();
            }
            catch { }
        }
    }
    
    protected virtual void AddHubHandlers() { }


    public async ValueTask DisposeAsync() => await Hub.DisposeAsync();
}