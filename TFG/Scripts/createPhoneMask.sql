CREATE OR ALTER FUNCTION [dbo].[phoneMask](@res nvarchar(250))  
RETURNS nvarchar(11)
AS   
BEGIN
DECLARE @i int = 1
DECLARE @other nvarchar(250) = ''
WHILE @i <= len(@res) and len(@other)<11
BEGIN
	IF SUBSTRING(@res, @i, 1) LIKE '%[0-9]%'
	BEGIN
	IF len(@other)=3 OR len(@other)=7
	BEGIN
		select @other = @other + '-'
	END
		select @other = @other + SUBSTRING(@res, @i, 1)
	END
	select @i = @i + 1
END
RETURN @other
END;