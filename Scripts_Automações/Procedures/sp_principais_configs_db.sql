-- Drop da procedure se existir
IF OBJECT_ID('dbo.sp_DBInfo', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_DBInfo;
GO

CREATE PROCEDURE dbo.sp_DBInfo
AS
BEGIN
    SET NOCOUNT ON;

    -- ===========================
    PRINT '====== INFORMAÇÕES DA INSTÂNCIA DO SQL SERVER ======';
    SELECT 
        SERVERPROPERTY('MachineName') AS Nome_Servidor,
        SERVERPROPERTY('ServerName') AS Nome_Instancia,
        SERVERPROPERTY('Edition') AS Edicao,
        SERVERPROPERTY('ProductVersion') AS Versao_SQLServer,
        SERVERPROPERTY('ProductLevel') AS Nivel_Patch,
        SERVERPROPERTY('EngineEdition') AS Engine_Edition,
        cpu_count AS Quantidade_CPU_Logica
    FROM sys.dm_os_sys_info;

    -- ===========================
    PRINT '====== CONFIGURAÇÕES GERAIS DO SERVIDOR ======';
    SELECT 
        name AS Configuracao,
        value_in_use AS Valor_Em_Uso
    FROM sys.configurations
    WHERE name IN (
        'max server memory (MB)', 
        'min server memory (MB)', 
        'max degree of parallelism', 
        'cost threshold for parallelism',
        'backup compression default',
        'remote query timeout',
        'optimize for ad hoc workloads'
    )
    ORDER BY name;

    -- ===========================
    PRINT '====== INFORMAÇÕES DO BANCO DE DADOS ATUAL (INCLUINDO TAMANHO E ESTATÍSTICAS) ======';
    SELECT 
        db.name AS Nome_Banco,
        db.state_desc AS Estado,
        db.recovery_model_desc AS Modo_Recuperacao,
        db.containment_desc AS Contencao,
        db.compatibility_level AS Nivel_Compatibilidade,
        db.create_date AS Data_Criacao,
        CAST(SUM(mf.size) * 8 / 1024 AS DECIMAL(10,2)) AS Tamanho_Total_MB,
        db.is_auto_update_stats_on AS Auto_Update_Statistics_Ativo,
        db.is_auto_update_stats_async_on AS Auto_Update_Statistics_Assincrono
    FROM sys.databases AS db
    INNER JOIN sys.master_files AS mf ON db.database_id = mf.database_id
    WHERE db.database_id = DB_ID() -- Somente o banco atual
    GROUP BY 
        db.name,
        db.state_desc,
        db.recovery_model_desc,
        db.containment_desc,
        db.compatibility_level,
        db.create_date,
        db.is_auto_update_stats_on,
        db.is_auto_update_stats_async_on;

END;
GO
