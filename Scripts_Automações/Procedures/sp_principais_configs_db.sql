-- Drop da procedure se existir
IF OBJECT_ID('dbo.sp_Informacoes_SQLServer', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Informacoes_SQLServer;
GO

CREATE PROCEDURE dbo.sp_Informacoes_SQLServer
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
    PRINT '====== INFORMAÇÕES DO BANCO DE DADOS ATUAL (INCLUINDO TAMANHO) ======';
    SELECT 
        db.name AS Nome_Banco,
        db.state_desc AS Estado,
        db.recovery_model_desc AS Modo_Recuperacao,
        db.containment_desc AS Contencao,
        db.compatibility_level AS Nivel_Compatibilidade,
        db.create_date AS Data_Criacao,
        CAST(SUM(size) * 8 / 1024 AS VARCHAR(20)) + ' MB' AS Tamanho_MB
    FROM sys.databases AS db
    JOIN sys.master_files AS mf ON db.database_id = mf.database_id
    WHERE db.name = DB_NAME()
    GROUP BY 
        db.name,
        db.state_desc,
        db.recovery_model_desc,
        db.containment_desc,
        db.compatibility_level,
        db.create_date;

    -- ===========================
    PRINT '====== LOCALIZAÇÃO DOS ARQUIVOS DO BANCO DE DADOS ATUAL ======';
    SELECT 
        name AS Nome_Arquivo,
        physical_name AS Caminho_Fisico,
        type_desc AS Tipo_Arquivo,
        CAST(size * 8 / 1024 AS VARCHAR(20)) + ' MB' AS Tamanho_Arquivo_MB
    FROM sys.master_files
    WHERE database_id = DB_ID();

END;
GO
