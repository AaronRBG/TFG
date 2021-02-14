CREATE OR ALTER FUNCTION [dbo].[DNIMask](@res nvarchar(250))  
RETURNS nvarchar(9)
AS   
BEGIN
DECLARE @i int = 1
DECLARE @other nvarchar(250) = ''
WHILE @i <= len(@res) and len(@other)<9
BEGIN
	IF len(@other)<8 and SUBSTRING(@res, @i, 1) LIKE '%[0-9]%'
		BEGIN
			select @other = @other + SUBSTRING(@res, @i, 1)
			select @i = @i + 1
		END
	ELSE IF len(@other)=8 and SUBSTRING(@res, @i, 1) LIKE '%[a-Z]%'
	BEGIN
		select @other = @other + upper(SUBSTRING(@res, @i, 1))
		select @i = @i + 1
	END
	ELSE
	BEGIN
		select @i = @i + 1
	END
END
RETURN @other
END;