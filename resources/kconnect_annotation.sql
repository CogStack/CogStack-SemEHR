USE [_DATABASE_NAME_]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[kconnect_annotations](
	[Ann_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[ann_run_seq] [int] NULL,
	[CN_Doc_ID] [varchar](200) NOT NULL,
	[start_offset] [int] NOT NULL,
	[end_offset] [int] NOT NULL,
	[ann_type] [varchar](50) NULL,
	[inst_major_type] [varchar](50) NOT NULL,
	[inst_minor_type] [varchar](50) NULL,
	[experiencer] [varchar](50) NULL,
	[inst_uri] [varchar](50) NULL,
	[string_orig] [varchar](100) NULL,
	[tui_uri] [varchar](2500) NULL,
	[pref_label] [varchar](500) NULL,
	[STY] [varchar](500) NULL,
	[negation] [varchar](150) NULL,
	[temporality] [varchar](50) NULL,
	[vocabularies] [text] NULL,
	[llID] [varchar](50) NULL,
	[created_date] [datetime] NOT NULL,
	[brcid] [varchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[Ann_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_brcid]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_brcid] ON [dbo].[kconnect_annotations] 
(
	[brcid] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_cui]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_cui] ON [dbo].[kconnect_annotations] 
(
	[inst_uri] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_docid]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_docid] ON [dbo].[kconnect_annotations] 
(
	[CN_Doc_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_exp]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_exp] ON [dbo].[kconnect_annotations] 
(
	[experiencer] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_neg]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_neg] ON [dbo].[kconnect_annotations] 
(
	[negation] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_sty]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_sty] ON [dbo].[kconnect_annotations] 
(
	[STY] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


USE [_DATABASE_NAME_]
/****** Object:  Index [IX_kconnect_temp]    Script Date: 01/05/2018 13:31:21 ******/
CREATE NONCLUSTERED INDEX [IX_kconnect_temp] ON [dbo].[kconnect_annotations] 
(
	[temporality] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

ALTER TABLE [dbo].[kconnect_annotations] ADD  CONSTRAINT [DF_kconnect_annotations_inst_major_type]  DEFAULT ('-') FOR [inst_major_type]
GO

ALTER TABLE [dbo].[kconnect_annotations] ADD  CONSTRAINT [DF_kconnect_annotations_created_date]  DEFAULT (getdate()) FOR [created_date]
GO


