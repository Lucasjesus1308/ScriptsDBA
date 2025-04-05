u.topsql avgwork ,10,10
tras as querys que consomem mais recursos

u.klhdl 0x020000006B5AE521CB41F9B601F5D87E0E31B6C6D8FDB3EF0000000000000000000000000000000000000000
--monta kill baseado no sql handle

u.le3 
--lista exec ativas

u.lea2xsql
--lista exec ativas no banco

u.leaqtexec2
--tras as querys que mais estao sendo exec pelo sql handle

[u].[pctdone]
--tras em % oq esta sendo exec(operaçoes:ex: bckp)

[u].[planget] 0x06000600d17bab37e0224d33dd02000001000000000000000000000000000000000000000000000000000000
--gera arquivo xml(plan execução)

[u].[sql] 0x02000000d17bab37a1a9eec8821e733e5e7432e32132ff240000000000000000000000000000000000000000
-- trazer as metricas trazendo sql handle como parametro

[u].[sqlfind] '%select%'
-- ele vai procuar por um pedaço de query, qro achar as metricas desse select(quando n tenho sql handle)

u.tbcol aluno
--tras as coluns da table que estou passando

u.tbfindlistalter aluno
-- ultimas alteraçoes no banco

u.tbfk
--lista as fks

[u].[tbfkref]
--que faz referencia a tabela

[u].[tbgrant] aluno
--aparecer os grants da tabela

[u].[tbidx]
---listar index

[u].[tbidxfrag]
--lista a porcentagem de frag de um index em uma table

[u].[tbsocupa]
--dar tamanho tablespace e onde estão armazenados

[u].[tbstat]
--lista statisticas da tabela

[u].[tbtbs] aluno
--desconsidera

[u].[tbtrg]
-- lista as triggers da tabela

[u].[topsql]








