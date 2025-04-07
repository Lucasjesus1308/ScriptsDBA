-- Drop da procedure se existir
IF OBJECT_ID('dbo.sp_CheckStats', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Estatisticas_Desatualizadas;
GO

CREATE PROCEDURE dbo.sp_CheckStats
AS
BEGIN
    SET NOCOUNT ON;

    -- Criação da tabela temporária para armazenar resultados
    IF OBJECT_ID('tempdb..#Estatisticas_Desatualizadas') IS NOT NULL
        DROP TABLE #Estatisticas_Desatualizadas;

    CREATE TABLE #Estatisticas_Desatualizadas (
        Nome_Banco SYSNAME,
        Schema_Nome SYSNAME,
        Tabela_Nome SYSNAME,
        Estatistica_Nome SYSNAME,
        Total_Linhas_Tabela BIGINT,
        Modificacoes_Desde_Ultima_Atualizacao BIGINT,
        Percentual_Modificacoes DECIMAL(10,2),
        Comando_Atualizacao NVARCHAR(MAX)
    );

    INSERT INTO #Estatisticas_Desatualizadas (
        Nome_Banco,
        Schema_Nome,
        Tabela_Nome,
        Estatistica_Nome,
        Total_Linhas_Tabela,
        Modificacoes_Desde_Ultima_Atualizacao,
        Percentual_Modificacoes,
        Comando_Atualizacao
    )
    SELECT
        DB_NAME() AS Nome_Banco,
        s.name AS Schema_Nome,
        t.name AS Tabela_Nome,
        st.name AS Estatistica_Nome,
        sp.rows AS Total_Linhas_Tabela,
        sp.modification_counter AS Modificacoes_Desde_Ultima_Atualizacao,
        CAST(sp.modification_counter AS FLOAT) / NULLIF(sp.rows, 0) * 100 AS Percentual_Modificacoes,
        'UPDATE STATISTICS [' + s.name + '].[' + t.name + '] [' + st.name + '];' AS Comando_Atualizacao
    FROM sys.stats AS st
    INNER JOIN sys.objects AS t ON st.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
    WHERE 
        t.type = 'U' -- Somente tabelas do usuário
        AND sp.rows > 0 -- Evita divisão por zero
        AND (CAST(sp.modification_counter AS FLOAT) / NULLIF(sp.rows, 0)) * 100 >= 0.5; -- Alterações >= 0,5%

    -- Resultado final
    SELECT * FROM #Estatisticas_Desatualizadas;

    -- Limpeza opcional da tabela temporária
    DROP TABLE #Estatisticas_Desatualizadas;
END;
GO
