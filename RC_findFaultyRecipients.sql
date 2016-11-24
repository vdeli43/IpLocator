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
-- Create date: 24-11-2016
-- Description:	Find Non UK Recipients that are assgned to UK
-- =============================================
CREATE PROCEDURE #RC_findFaultyRecipients 
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    -- Insert statements for procedure here
	SELECT #RecipientsRealCountry.id, #RecipientsRealCountry.country
		FROM #RecipientsRealCountry
	INNER JOIN Recipients
		ON #RecipientsRealCountry.id = Recipients.id
	WHERE  #RecipientsRealCountry.country != 'GB'  and Recipients.countryFile = 'UK'
END

