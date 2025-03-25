CREATE TABLE dbo.Backup_Log (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    DatabaseName NVARCHAR(200),
    BackupPath NVARCHAR(500),
    BackupDate DATETIME DEFAULT GETDATE(),
    VerifyStatus NVARCHAR(50),
    ErrorMessage NVARCHAR(MAX),
	BackupType NVARCHAR(50));

SELECT * FROM dbo.Backup_Log ORDER BY BackupDate DESC;


EXEC SP_BACKUP_FULL @CAMINHO = 'C:\Users\marcelo.jesus\Documents\TESTEBACKUP\bkp';
CREATE PROCEDURE SP_BACKUP_FULL (@CAMINHO VARCHAR(500))
AS
BEGIN
    DECLARE @ARQUIVO VARCHAR(500), @FULLPATH VARCHAR(500), @STATUS NVARCHAR(50), @ERROR_MSG NVARCHAR(MAX)

    -- Gerar nome do arquivo de backup com base na data e hora
    SELECT @ARQUIVO = 'Testebackup_FULL_' + 
        FORMAT(GETDATE(), 'dd_MM_HH_mm') + '.bak'

    -- Concatenar o caminho completo para o arquivo de backup
    SET @FULLPATH = @CAMINHO + @ARQUIVO

    BEGIN TRY
        -- Fazendo o backup completo
        BACKUP DATABASE testebackup
        TO DISK = @FULLPATH
        WITH COMPRESSION, CHECKSUM

        -- Verificando o backup
        RESTORE VERIFYONLY FROM DISK = @FULLPATH

        -- Se passou na verificação
        SET @STATUS = 'VALID'
        SET @ERROR_MSG = NULL
    END TRY
    BEGIN CATCH
        -- Se der erro na verificação
        SET @STATUS = 'FAILED'
        SET @ERROR_MSG = ERROR_MESSAGE()
    END CATCH

    -- Registrando no log com o tipo de backup (FULL)
    INSERT INTO dbo.Backup_Log (DatabaseName, BackupPath, VerifyStatus, ErrorMessage, BackupType)
    VALUES ('testebackup', @FULLPATH, @STATUS, @ERROR_MSG, 'FULL')
END



---------------------------------------------------------PROC BACKUP DIFF-----------------------------------


EXEC SP_BACKUP_DIFF @CAMINHO = 'C:\Users\marcelo.jesus\Documents\TESTEBACKUP\bkp';

CREATE PROCEDURE SP_BACKUP_DIFF (@CAMINHO VARCHAR(500))
AS
BEGIN
    DECLARE @ARQUIVO VARCHAR(500), @FULLPATH VARCHAR(500), @STATUS NVARCHAR(50), @ERROR_MSG NVARCHAR(MAX)

    -- Gerar nome do arquivo de backup com base na data e hora
    SELECT @ARQUIVO = 'Testebackup_DIFF_' + 
        FORMAT(GETDATE(), 'dd_MM_HH_mm') + '.bak'

    -- Concatenar o caminho completo para o arquivo de backup
    SET @FULLPATH = @CAMINHO + @ARQUIVO

    BEGIN TRY
        -- Fazendo o backup diferencial
        BACKUP DATABASE testebackup
        TO DISK = @FULLPATH
        WITH DIFFERENTIAL, COMPRESSION, CHECKSUM

        -- Verificando o backup
        RESTORE VERIFYONLY FROM DISK = @FULLPATH

        -- Se passou na verificação
        SET @STATUS = 'VALID'
        SET @ERROR_MSG = NULL
    END TRY
    BEGIN CATCH
        -- Se der erro na verificação
        SET @STATUS = 'FAILED'
        SET @ERROR_MSG = ERROR_MESSAGE()
    END CATCH

    -- Registrando no log com o tipo de backup (DIFF)
    INSERT INTO dbo.Backup_Log (DatabaseName, BackupPath, VerifyStatus, ErrorMessage, BackupType)
    VALUES ('testebackup', @FULLPATH, @STATUS, @ERROR_MSG, 'DIFF')
END


---------------------------------------------------------PROC BACKUP LOG----------------------------------

CREATE PROCEDURE SP_BACKUP_LOG (@CAMINHO VARCHAR(500))
AS
BEGIN
    DECLARE @ARQUIVO VARCHAR(500), @FULLPATH VARCHAR(500), @STATUS NVARCHAR(50), @ERROR_MSG NVARCHAR(MAX)

    -- Gerar nome do arquivo de backup com base na data e hora
    SELECT @ARQUIVO = 'Testebackup_LOG_' + 
        FORMAT(GETDATE(), 'dd_MM_HH_mm') + '.trn'

    -- Concatenar o caminho completo para o arquivo de backup
    SET @FULLPATH = @CAMINHO + @ARQUIVO

    BEGIN TRY
        -- Fazendo o backup do log
        BACKUP LOG testebackup
        TO DISK = @FULLPATH
        WITH COMPRESSION, CHECKSUM

        -- Verificando o backup
        RESTORE VERIFYONLY FROM DISK = @FULLPATH

        -- Se passou na verificação
        SET @STATUS = 'VALID'
        SET @ERROR_MSG = NULL
    END TRY
    BEGIN CATCH
        -- Se der erro na verificação
        SET @STATUS = 'FAILED'
        SET @ERROR_MSG = ERROR_MESSAGE()
    END CATCH

    -- Registrando no log com o tipo de backup (LOG)
    INSERT INTO dbo.Backup_Log (DatabaseName, BackupPath, VerifyStatus, ErrorMessage, BackupType)
    VALUES ('testebackup', @FULLPATH, @STATUS, @ERROR_MSG, 'LOG')
END
