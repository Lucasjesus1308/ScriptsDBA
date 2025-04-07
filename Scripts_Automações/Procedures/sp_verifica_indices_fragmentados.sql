-- Drop da procedure se existir
IF OBJECT_ID('dbo.sp_checkidx', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_checkidx;
GO

CREATE PROCEDURE dbo.sp_checkidx
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '====== MONITORAMENTO PREMIUM DE FRAGMENTAÇÃO DE ÍNDICES ======';

    -- Coleta índices fragmentados com relevância (índices grandes)
    SELECT
        DB_NAME() AS Nome_Banco,
        OBJECT_SCHEMA_NAME(ips.object_id) AS Nome_Schema,
        OBJECT_NAME(ips.object_id) AS Nome_Tabela,
        i.name AS Nome_Indice,
        ips.index_id AS ID_Indice,
        ips.avg_fragmentation_in_percent AS Percentual_Fragmentacao,
        ips.page_count AS Quantidade_Paginas,
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 'Rebuild Recomendado'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 'Reorganize Recomendado'
            ELSE 'Nenhuma ação necessária'
        END AS Recomendacao,
        -- Comando para Enterprise (com ONLINE = ON)
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = ON);'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
            ELSE ''
        END AS Comando_Enterprise,
        -- Comando para versões Standard (sem ONLINE = ON)
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= 30 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REBUILD;'
            WHEN ips.avg_fragmentation_in_percent >= 5 THEN 
                'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
            ELSE ''
        END AS Comando_Standard
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE 
        ips.index_id > 0 -- Ignora heaps (sem índice)
        AND ips.page_count > 100 -- Filtro: pega só índices com mais de 100 páginas
        AND ips.avg_fragmentation_in_percent >= 5 -- Fragmentação relevante
    ORDER BY ips.avg_fragmentation_in_percent DESC;
END;
GO
