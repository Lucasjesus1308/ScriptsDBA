-- Drop da procedure se existir
IF OBJECT_ID('dbo.sp_Monitoramento_Indices_Fragmentados', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Monitoramento_Indices_Fragmentados;
GO

CREATE PROCEDURE dbo.sp_Monitoramento_Indices_Fragmentados
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '====== MONITORAMENTO DE FRAGMENTAÇÃO DE ÍNDICES (SOMENTE ÍNDICES COM NECESSIDADE DE MANUTENÇÃO) ======';

    SELECT
        DB_NAME() AS Nome_Banco,
        OBJECT_NAME(ips.object_id) AS Nome_Tabela,
        i.name AS Nome_Indice,
        ips.index_id AS ID_Indice,
        ips.avg_fragmentation_in_percent AS Percentual_Fragmentacao,
        ips.page_count AS Quantidade_Paginas,
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 'Rebuild Recomendado'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 'Reorganize Recomendado'
        END AS Recomendacao,
        -- Comando para Enterprise (com ONLINE = ON)
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = ON);'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
        END AS Comando_Manutencao,
        -- Comando para versões Standard e outras que não suportam ONLINE = ON
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REBUILD;'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
        END AS Comando_Manutencao_Standard
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') AS ips
    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE 
        ips.database_id = DB_ID()
        AND i.type_desc <> 'HEAP' -- Exclui tabelas HEAP (sem índice clustered)
        AND ips.avg_fragmentation_in_percent >= 5 -- Apenas índices que precisam de manutenção
    ORDER BY 
        ips.avg_fragmentation_in_percent DESC;
END;
GO
