CREATE OR ALTER FUNCTION [dbo].[emailMask](@res nvarchar(250))  
RETURNS nvarchar(250)
AS   
BEGIN
DECLARE @i int = 1
DECLARE @other nvarchar(250) = ''
DECLARE @at bit = 0
DECLARE @dot bit = 0
WHILE @i <= len(@res)
BEGIN
	IF SUBSTRING(@res, @i, 1) LIKE '%[a-Z]%'
	BEGIN
		select @other = @other + SUBSTRING(@res, @i, 1)
	END
	ELSE IF SUBSTRING(@res, @i, 1) LIKE '%[0-9]%' AND @at = 0
	BEGIN
		select @other = @other + SUBSTRING(@res, @i, 1)
	END
	ELSE IF SUBSTRING(@res, @i, 1) = '@' and @at = 0
	BEGIN
		select @other = @other + SUBSTRING(@res, @i, 1)
		select @at = 1
	END
	ELSE IF SUBSTRING(@res, @i, 1) = '.' and @at = 1 and @dot = 0
	BEGIN
		select @other = @other + SUBSTRING(@res, @i, 1)
		select @dot = 1
	END
	select @i = @i + 1
END
RETURN @other
END;