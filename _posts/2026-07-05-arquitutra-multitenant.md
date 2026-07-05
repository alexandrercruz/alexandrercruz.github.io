---
title: Arquitetura Multitenant
tags: [PostgreSQL, .NET, C#]
style: 
color: 
description: Gerenciamento de conexões multitenant com EF Core e PostgreSQL no .NET
---

# Gerenciamento de conexões multitenant com EF Core e PostgreSQL no .NET

Quando cada cliente tem seu próprio banco de dados, a pergunta que define a performance da aplicação não é "como escrever queries eficientes", mas sim "como gerenciar 10 mil pools de conexão sem explodir o servidor".

Este artigo documenta uma arquitetura para aplicações .NET multitenant com banco separado por cliente, usando EF Core e PostgreSQL, projetada para suportar 10 mil tenants com picos de 5.000 requisições por segundo.

---

## O problema

Em arquiteturas multitenant com banco separado por cliente, cada tenant precisa de sua própria connection string. O EF Core, por padrão, é configurado uma vez na inicialização da aplicação — o que não funciona quando você tem 10 mil connection strings diferentes.

As abordagens ingênuas criam dois problemas opostos:

- **Criar um DbContext novo a cada request** sem reaproveitamento de pool → abre e fecha conexões físicas constantemente, destruindo a performance
- **Manter um DbContext por tenant em memória** → com 10 mil tenants, isso significa milhares de conexões físicas abertas no PostgreSQL mesmo sem nenhuma requisição ativa

A solução está em separar o ciclo de vida do **pool de conexões** do ciclo de vida do **DbContext**.

---

## A ideia central

O Npgsql, driver oficial do PostgreSQL para .NET, mantém um pool de conexões físicas por `NpgsqlDataSource`. Esse pool **vive no processo**, independente do DbContext. Quando o DbContext é descartado, ele apenas devolve a conexão ao pool — não fecha nada.

Isso significa que podemos:

1. Manter um `NpgsqlDataSource` por tenant em cache, com expiração por inatividade
2. Criar e descartar DbContexts livremente a cada request, sem custo de abertura de conexão
3. Fechar o pool inteiro de um tenant quando ele ficar inativo, liberando recursos no servidor

---

## Arquitetura

A solução é composta por quatro responsabilidades bem separadas:

```
Request HTTP
    │
    ▼
[ TenantResolver ]          Quem é o tenant desta requisição?
    │
    ▼
[ TenantDbContextFactory ]  Existe pool ativo para este tenant?
    │
    ├── SIM → retorna DataSource do cache
    │
    └── NÃO → [ TenantConnectionStore ] Qual servidor atende este tenant?
                    │
                    ▼
              [ NpgsqlDataSource ] Cria pool isolado e armazena no cache
                    │
                    ▼
              [ AppDbContext ] Criado com o DataSource do tenant
```

---

## Identificando o tenant

O primeiro passo de qualquer requisição é saber qual tenant está fazendo a chamada. Isso é feito pelo `TenantResolver`:

```csharp
public interface ITenantResolver
{
    string GetTenantId();
}

public class HttpTenantResolver(IHttpContextAccessor accessor) : ITenantResolver
{
    public string GetTenantId() =>
        accessor.HttpContext?.User.FindFirst("tenant_id")?.Value
        ?? accessor.HttpContext?.Request.Headers["X-Tenant-Id"].ToString()
        ?? throw new InvalidOperationException("Tenant não identificado");
}
```

A resolução tenta duas fontes em ordem de prioridade:

1. **Claim `tenant_id` do JWT** — ideal para usuários autenticados onde o tenant faz parte da identidade
2. **Header `X-Tenant-Id`** — útil para chamadas machine-to-machine ou APIs sem autenticação JWT

Lançar exceção quando nenhum dos dois está presente é intencional: impede que dados de um tenant vazem para outro por ausência de contexto.

A interface permite trocar a estratégia sem alterar nada mais. Em um worker background que processa filas, por exemplo, você implementaria uma versão que lê o tenant_id da mensagem.

---

## Distribuindo tenants entre servidores

Com 10 mil tenants e 5 servidores PostgreSQL, é preciso decidir qual servidor atende cada tenant. O `TenantConnectionStore` faz isso via **hash consistente**:

```csharp
public class TenantConnectionStore(IConfiguration config) : ITenantConnectionStore
{
    private readonly string[] _servers = config
        .GetSection("Tenants:Servers")
        .Get<string[]>() ?? throw new InvalidOperationException("Tenants:Servers não configurado");

    public Task<string> GetConnectionStringAsync(string tenantId)
    {
        var index = Math.Abs(tenantId.GetHashCode()) % _servers.Length;
        var server = _servers[index];
        return Task.FromResult($"{server};Database={tenantId}");
    }
}
```

O ponto crítico aqui é que o **mesmo tenant sempre vai para o mesmo servidor**. Se o tenant fosse para servidores diferentes a cada request, o Npgsql criaria pools duplicados — um por servidor — multiplicando o consumo de conexões sem nenhum benefício.

O `Math.Abs` é necessário porque `GetHashCode()` pode retornar valores negativos, o que causaria um índice inválido no array.

A configuração dos servidores fica no `appsettings.json`:

```json
{
  "Tenants": {
    "Servers": [
      "Host=pg-01;Username=app;Password=<password>",
      "Host=pg-02;Username=app;Password=<password>",
      "Host=pg-03;Username=app;Password=<password>",
      "Host=pg-04;Username=app;Password=<password>",
      "Host=pg-05;Username=app;Password=<password>"
    ]
  }
}
```

---

## Gerenciando o pool de conexões

A `TenantDbContextFactory` é onde a mágica acontece. Ela mantém um `NpgsqlDataSource` por tenant em cache e cria DbContexts sob demanda:

```csharp
public class TenantDbContextFactory(
    ITenantResolver tenantResolver,
    ITenantConnectionStore connectionStore,
    IMemoryCache cache)
{
    private static readonly MemoryCacheEntryOptions _cacheOptions = new MemoryCacheEntryOptions()
        .SetSize(1)
        .SetSlidingExpiration(TimeSpan.FromMinutes(20))
        .SetAbsoluteExpiration(TimeSpan.FromHours(2))
        .RegisterPostEvictionCallback((_, value, _, _) =>
            (value as NpgsqlDataSource)?.Dispose());

    public async Task<AppDbContext> CreateAsync()
    {
        var tenantId = tenantResolver.GetTenantId();
        var dataSource = await GetOrCreateDataSourceAsync(tenantId);

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(dataSource, npgsql => npgsql.CommandTimeout(2))
            .Options;

        return new AppDbContext(options);
    }

    private async Task<NpgsqlDataSource> GetOrCreateDataSourceAsync(string tenantId)
    {
        if (cache.TryGetValue(tenantId, out NpgsqlDataSource? cached))
            return cached!;

        var cs = await connectionStore.GetConnectionStringAsync(tenantId);
        var dataSource = new NpgsqlDataSourceBuilder(cs)
            .ConfigureDataSource(o =>
            {
                o.MinPoolSize = 0;
                o.MaxPoolSize = 5;
                o.ConnectionIdleLifetime = 120;
                o.ConnectionPruningInterval = 30;
            })
            .Build();

        return cache.Set(tenantId, dataSource, _cacheOptions);
    }
}
```

### Por que NpgsqlDataSource e não connection string direta?

Antes do Npgsql 7, o pool era gerenciado implicitamente por connection string — o driver mantinha um dicionário estático interno. Isso funcionava, mas sem controle algum sobre o ciclo de vida.

Com `NpgsqlDataSource`, você tem controle explícito: sabe exatamente quando o pool é criado, como está configurado e quando é destruído. O `PostEvictionCallback` é a prova disso — quando o cache expira a entrada de um tenant, o pool é fechado imediatamente e as conexões físicas são liberadas no servidor.

### O papel do cache

O `MemoryCache` aqui não é apenas uma otimização — é o mecanismo de controle de recursos. Cada entrada representa um tenant com pool ativo. As configurações de expiração foram calculadas para a janela de pico das 18h às 22h:

- **SlidingExpiration de 20 minutos**: tenants que estão fazendo requisições continuamente nunca expiram durante o pico
- **AbsoluteExpiration de 2 horas**: força renovação periódica, evitando que tenants de sessão muito longa acumulem estado
- **SizeLimit de 1.500**: limita o número de pools simultâneos, protegendo a memória da aplicação

Quando o cache atinge 1.500 entradas e precisa adicionar uma nova, ele descarta automaticamente as menos usadas (LRU), disparando o callback que fecha o pool correspondente.

### Configuração do pool

| Parâmetro | Valor | Raciocínio |
|---|---|---|
| `MinPoolSize` | 0 | Sem conexões ociosas — o pool só abre sob demanda |
| `MaxPoolSize` | 5 | 1.500 tenants × 5 = 7.500 conexões, dentro das 9.000 disponíveis |
| `ConnectionIdleLifetime` | 120s | Fecha conexões não usadas após 2 minutos |
| `ConnectionPruningInterval` | 30s | Frequência de varredura do pool |
| `CommandTimeout` | 2s | Evita que queries lentas esgotem o pool do tenant |

O `MinPoolSize = 0` é o ajuste mais impactante. Com 10 mil tenants, qualquer valor acima de zero significa conexões físicas abertas em tenants que podem não ter nenhuma requisição ativa há horas.

---

## Capacidade calculada

Com 5 servidores e 2.000 conexões cada:

```
Conexões totais:       5 × 2.000 = 10.000
Reserva admin (10%):            -  1.000
Disponível:                        9.000

Pico de 5.000 req/s × 500ms duração média = 2.500 conexões simultâneas ativas
Com margem de segurança 2x:                  5.000 conexões planejadas

1.500 tenants ativos × 5 conexões máximas = 7.500 conexões máximas teóricas
Headroom disponível:   9.000 - 7.500      = 1.500 conexões de folga
```

A folga de 1.500 conexões absorve rajadas onde múltiplos tenants atingem o `MaxPoolSize` simultaneamente.

---

## Registro no DI

```csharp
public static class ServiceExtensions
{
    public static IServiceCollection AddMultitenant(this IServiceCollection services)
    {
        services.AddMemoryCache(o => o.SizeLimit = 1500);
        services.AddHttpContextAccessor();
        services.AddSingleton<ITenantConnectionStore, TenantConnectionStore>();
        services.AddScoped<ITenantResolver, HttpTenantResolver>();
        services.AddScoped<TenantDbContextFactory>();
        return services;
    }
}
```

Os lifetimes são intencionais:

- `TenantConnectionStore` como **Singleton** — não tem estado mutável, apenas lê configuração
- `HttpTenantResolver` como **Scoped** — precisa do HttpContext, que é por request
- `TenantDbContextFactory` como **Scoped** — depende do resolver que é Scoped, não pode ser Singleton

---

## Usando nos serviços

```csharp
public class PedidoService(TenantDbContextFactory factory)
{
    public async Task<List<Pedido>> ListarAsync()
    {
        await using var db = await factory.CreateAsync();
        return await db.Pedidos.AsNoTracking().ToListAsync();
    }
}
```

O `await using` garante que o `Dispose()` do DbContext seja chamado ao final do bloco, devolvendo a conexão ao pool. O `AsNoTracking()` é recomendado em queries de leitura — elimina o overhead do change tracker do EF Core, que rastreia alterações em entidades para o `SaveChanges()`.

---

## Armadilhas comuns

**Connection string com formatação inconsistente**

O Npgsql identifica o pool pela connection string exata. Uma diferença de espaço ou ponto-e-vírgula no final cria um pool separado:

```csharp
// Estes dois criam POOLS DIFERENTES
"Host=db;Database=tenant_1;Username=app"
"Host=db;Database=tenant_1;Username=app;" // trailing semicolon
```

Sempre monte a connection string de forma determinística, como feito no `TenantConnectionStore`.

**Race condition no primeiro acesso**

Se dois requests do mesmo tenant chegarem simultaneamente e ambos derem cache miss, dois `NpgsqlDataSource` serão criados. O `cache.Set` é thread-safe e o segundo sobrescreve o primeiro, mas o primeiro não é descartado automaticamente — fica órfão em memória até o GC coletar.

Para ambientes com altíssima concorrência no primeiro acesso, considere usar `SemaphoreSlim` por tenant para serializar a criação.

**Tenant inativo com pool aberto**

Sem o `PostEvictionCallback`, descartar a entrada do cache não fecha o pool. O `NpgsqlDataSource` continuaria vivo em memória com conexões físicas abertas no servidor. Sempre registre o callback ao criar entradas de cache que encapsulam recursos não gerenciados.

---

## Conclusão

A chave desta arquitetura é entender que o **pool de conexões e o DbContext têm ciclos de vida completamente diferentes**. O pool deve ser longo e gerenciado ativamente. O DbContext deve ser curto e descartado a cada operação.

Separar essas responsabilidades em `NpgsqlDataSource` (pool) e `AppDbContext` (sessão) permite escalar para 10 mil tenants sem desperdiçar conexões em tenants inativos e sem o custo de abrir conexões físicas a cada request.
