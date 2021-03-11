CREATE OR ALTER FUNCTION [dbo].[creditCardMask](@res nvarchar(250))  
RETURNS nvarchar(19)
AS   
BEGIN
DECLARE @i int = 1
DECLARE @other nvarchar(250) = ''
WHILE @i <= len(@res) and len(@other)<19
BEGIN
	IF SUBSTRING(@res, @i, 1) LIKE '%[0-9]%'
	BEGIN
	IF len(@other)=4 OR len(@other)=9 OR len(@other)=14
	BEGIN
		select @other = @other + '-'
	END
		select @other = @other + SUBSTRING(@res, @i, 1)
	END
	select @i = @i + 1
END
RETURN @other
END;