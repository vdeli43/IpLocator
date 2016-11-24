-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
-- =============================================
-- Author:		Vasilis Delivorias
-- Create date: 23/11/2016
-- Description:	Get Click/ Open Actions
-- =============================================
CREATE PROCEDURE #RC_getActions 
	-- Add the parameters for the stored procedure here
	-- actionType corresponds to the table to be used: CKClicks or CKOpens
	-- countRecords=TRUE, just count the records for the given dates, else get the records
	@actionType VARCHAR(50) = 'Clicks',
	@fromDate DATETIME = NULL, 
	@toDate DATETIME = NULL,
	@countRecords BIT = 'FALSE'
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    -- Insert statements for procedure here

	DECLARE @FDate DATETIME
	DECLARE @TDate DATETIME

	SET @FDate = ISNULL(@fromDate, CONVERT(DATE,'2000-01-01',121))
	SET @TDate = ISNULL(@toDate, GETDATE())

	IF @countRecords = 'TRUE'
		IF @actionType = 'Clicks'
			SELECT count(*) FROM CKClicks
				WHERE click_date BETWEEN @FDate AND @TDate
				AND subid2 IS NOT NULL
		ELSE IF @actionType = 'Opens'
			SELECT count(*) FROM CKOpens
				WHERE tmstamp BETWEEN @FDate AND @TDate
				AND subid2 IS NOT NULL
		ELSE
			SELECT 0
	ELSE
		IF @actionType = 'Clicks'
			SELECT subid2, ip_address FROM CKClicks
				WHERE click_date BETWEEN @FDate AND @TDate
				AND subid2 IS NOT NULL
		ELSE IF @actionType = 'Opens'
			SELECT subid2, ip_address FROM CKOpens
				WHERE tmstamp BETWEEN @FDate AND @TDate
				AND subid2 IS NOT NULL
		ELSE
			SELECT 0
	--SELECT @FDate, @TDate
END

