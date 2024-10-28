IF NOT EXISTS (SELECT * FROM sys.tables WHERE [name] = 'tb_letter_generation_templates')
BEGIN
	CREATE TABLE [dbo].[tb_letter_generation_templates](
		[id_template] [int] IDENTITY(1,1) NOT NULL,
		[id_scope] [int] NULL,
		[name] [varchar](150) NOT NULL,
		[comments] [varchar](1000) NULL,

		CONSTRAINT [PK_tb_letter_generation_templates_id] PRIMARY KEY CLUSTERED (id_template),
		CONSTRAINT [UN_tb_letter_generation_templates_name] UNIQUE NONCLUSTERED ([name]),
		CONSTRAINT [FK_tb_letter_generation_templates_id_scope] FOREIGN KEY ([id_scope]) REFERENCES _tb_bqm_ref_MI_Scope(scope_id)
	)
END
GO



IF NOT EXISTS (SELECT * FROM sys.tables WHERE [name] = 'tb_letter_generation_main_section_details')
BEGIN
	CREATE TABLE [dbo].[tb_letter_generation_main_section_details](
		[id] [int] IDENTITY(1,1) NOT NULL,
		[id_template] [int] NOT NULL,
		[id_section] [int] NULL,
		[code] [varchar](250) NOT NULL,
		[text] [varchar](max) NOT NULL,
		[display_condition] [int] NULL,
		[sort] [int] NOT NULL,
		[bold] [bit] NOT NULL,
		[underline] [bit] NOT NULL,
		[italic] [bit] NOT NULL,
		[text_align] [int] NOT NULL,
		[font_size] [int] NOT NULL,
		[breakline] [bit] NOT NULL,
		[padding_left] [int] NOT NULL,
		[padding_right] [int] NOT NULL,
		[padding_top] [int] NOT NULL,
		[padding_bottom] [int] NOT NULL,
		[detail_section] [varchar](10) NULL,
		CONSTRAINT [PK_tb_letter_generation_main_section_details_id] PRIMARY KEY CLUSTERED ([id] ASC),
		CONSTRAINT [FK_tb_letter_generation_main_section_details_id_template] FOREIGN KEY ([id_template]) REFERENCES tb_letter_generation_templates([id_template]),
		CONSTRAINT [FK_tb_letter_generation_main_section_details_display_condition] FOREIGN KEY ([display_condition]) REFERENCES tb_letter_generation_template_expressions([id])
	)
END
GO



IF NOT EXISTS (SELECT * FROM sys.tables WHERE [name] = 'tb_letter_generation_merit_increase_details')
BEGIN
	CREATE TABLE [dbo].[tb_letter_generation_merit_increase_details](
		[id] [int] IDENTITY(1,1) NOT NULL,
		[id_template] [int] NOT NULL,
		[id_section] [int] NOT NULL,
		[code] [varchar](250) NOT NULL,
		[text] [varchar](max) NULL,
		[display_condition] [int] NULL,
		[sort] [int] NOT NULL,
		[bold] [bit] NOT NULL,
		[underline] [bit] NOT NULL,
		[italic] [bit] NOT NULL,
		[text_align] [int] NOT NULL,
		[font_size] [int] NOT NULL,
		[breakline] [bit] NOT NULL,
		[padding_left] [int] NOT NULL,
		[padding_right] [int] NOT NULL,
		[padding_top] [int] NOT NULL,
		[padding_bottom] [int] NOT NULL,
		[detail_section] [varchar](10) NULL,
		CONSTRAINT [PK_tb_letter_generation_merit_increase_details_id] PRIMARY KEY CLUSTERED ([id] ASC),
		CONSTRAINT [FK_tb_letter_generation_merit_increase_details_id_template] FOREIGN KEY ([id_template]) REFERENCES tb_letter_generation_templates([id_template]),
		CONSTRAINT [FK_tb_letter_generation_merit_increase_details_id_section] FOREIGN KEY ([id_section]) REFERENCES tb_letter_generation_sections([id_section]),
		CONSTRAINT [FK_tb_letter_generation_merit_increase_details_display_condition] FOREIGN KEY ([display_condition]) REFERENCES tb_letter_generation_template_expressions([id])
	)
END
GO

	