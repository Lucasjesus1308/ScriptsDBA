CREATE TABLE Monitoramento_Estatisticas (
    DatabaseName SYSNAME,
    TableSchema SYSNAME,
    TableName SYSNAME,
    StatisticName SYSNAME,
    LastUpdated DATETIME,
    RowsModified BIGINT,
    TotalRows BIGINT,
    PercentModified FLOAT,
    DataColeta DATETIME,
	StatusAtualizacao VARCHAR(50)
);

-----------------------------------------PROC sp_VerificarEstatisticasDesatualizadas


CREATE OR ALTER PROCEDURE sp_VerificarEstatisticasDesatualizadas
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DatabaseName SYSNAME = DB_NAME();
    
    -- Criar tabela temporária para armazenar os resultados
    CREATE TABLE #Estatisticas (
        TableSchema SYSNAME,
        TableName SYSNAME,
        StatisticName SYSNAME,
        LastUpdated DATETIME,
        RowsModified BIGINT,
        TotalRows BIGINT
    );

    -- Capturar qualquer erro que ocorra durante a execução
    BEGIN TRY
        -- Consulta para pegar estatísticas desatualizadas
        INSERT INTO #Estatisticas
        SELECT  
            s.name AS TableSchema,
            t.name AS TableName,
            st.name AS StatisticName,
            sp.last_updated AS LastUpdated,
            sp.modification_counter AS RowsModified,
            p.rows AS TotalRows
        FROM sys.stats st
        JOIN sys.tables t ON st.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
        JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
        WHERE sp.modification_counter > (p.rows * 0.01);  -- Mais de 1% das linhas foram modificadas

        -- Inserir os dados na tabela de monitoramento
        INSERT INTO Traces.dbo.Monitoramento_Estatisticas
        (
            DatabaseName,
            TableSchema,
            TableName,
            StatisticName,
            LastUpdated,
            RowsModified,
            TotalRows,
            PercentModified,
            DataColeta,
            StatusAtualizacao  -- Coluna para o status de atualização
        )
        SELECT 
            @DatabaseName AS DatabaseName,
            TableSchema,
            TableName,
            StatisticName,
            -- Verificação e definição do campo LastUpdated
            CASE 
                WHEN LastUpdated IS NOT NULL AND LastUpdated != '' THEN LastUpdated
                ELSE NULL
            END AS LastUpdated,
            RowsModified,
            TotalRows,
            -- Calculando o percentual de linhas modificadas
            (RowsModified * 100.0 / NULLIF(TotalRows, 0)) AS PercentModified,
            GETDATE() AS DataColeta,
            'Pendente' AS StatusAtualizacao  -- Definindo o status como "Pendente"
        FROM #Estatisticas;

        PRINT 'Monitoramento de estatísticas desatualizadas concluído.';
    END TRY
    BEGIN CATCH
        -- Captura e imprime o erro caso aconteça
        PRINT 'Erro: ' + ERROR_MESSAGE();
        THROW;  -- Re-levanta o erro para que a execução seja interrompida
    END CATCH;

    -- Limpar tabela temporária
    DROP TABLE #Estatisticas;
END;
GO




-----------------------------------------sp_AtualizarEstatisticas


CREATE OR ALTER PROCEDURE sp_AtualizarEstatisticas
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TableSchema SYSNAME, @TableName SYSNAME;
    DECLARE @Sql NVARCHAR(MAX);
    
    -- Criar cursor para ler as tabelas que têm estatísticas pendentes
    DECLARE table_cursor CURSOR FOR
    SELECT TableSchema, TableName
    FROM Traces.dbo.Monitoramento_Estatisticas
    WHERE StatusAtualizacao = 'Pendente'; -- Só vai atualizar as estatísticas pendentes

    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @TableSchema, @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Construindo a instrução UPDATE STATISTICS dinamicamente
        SET @Sql = N'UPDATE STATISTICS ' + QUOTENAME(@TableSchema) + N'.' + QUOTENAME(@TableName);

        -- Executando a atualização de estatísticas
        EXEC sp_executesql @Sql;
        
        -- Atualizando o status para 'Atualizado' após realizar a atualização
        UPDATE Traces.dbo.Monitoramento_Estatisticas
        SET StatusAtualizacao = 'Atualizado', DataColeta = GETDATE()
        WHERE TableSchema = @TableSchema AND TableName = @TableName;

        FETCH NEXT FROM table_cursor INTO @TableSchema, @TableName;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;

    PRINT 'Estatísticas atualizadas e status de monitoramento atualizado.';
END;
GO
