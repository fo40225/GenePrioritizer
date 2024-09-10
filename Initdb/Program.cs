using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Xml.Linq;
using System.Xml;

var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables();

var configuration = builder.Build();

var connectionString = configuration.GetConnectionString("DefaultConnection");

SqlConnectionStringBuilder sbb = new SqlConnectionStringBuilder(connectionString);

var databaseName = sbb.InitialCatalog;
sbb.Remove("Initial Catalog");

var masterConnectionString = sbb.ConnectionString;

var dropDatabaseQuery = $"DROP DATABASE IF EXISTS {databaseName}";
var createDatabaseQuery = $"CREATE DATABASE {databaseName}";
var alterDatabaseQuery = $"ALTER DATABASE {databaseName} SET RECOVERY SIMPLE;";

using (SqlConnection connection = new SqlConnection(masterConnectionString))
{
    try
    {
        connection.Open();

        using (SqlCommand command = new SqlCommand(dropDatabaseQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"Database '{databaseName}' dropped successfully.");
        }

        using (SqlCommand command = new SqlCommand(createDatabaseQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"Database '{databaseName}' created successfully.");
        }

        using (SqlCommand command = new SqlCommand(alterDatabaseQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"Database '{databaseName}' recovery model changed to SIMPLE successfully.");
        }
    }
    catch (SqlException ex)
    {
        Console.WriteLine("An error occurred while creating the database: " + ex.Message);
        return 1;
    }
}

using (SqlConnection connection = new SqlConnection(connectionString))
{
    try
    {
        connection.Open();

        var sqlQuery = @"
CREATE TABLE [dbo].[hgnc_complete_set] (
    [symbol]    NVARCHAR (255) NOT NULL,
    [entrez_id] INT            NOT NULL
)

CREATE TABLE [dbo].[hgnc_complete_set_raw] (
 [hgnc_id]                                     NVARCHAR(4000) NOT NULL
,[symbol]                                      NVARCHAR(4000) NOT NULL
,[name]                                        NVARCHAR(4000) NOT NULL
,[locus_group]                                 NVARCHAR(4000) NOT NULL
,[locus_type]                                  NVARCHAR(4000) NOT NULL
,[status]                                      NVARCHAR(4000) NOT NULL
,[location]                                    NVARCHAR(4000) NOT NULL
,[location_sortable]                           NVARCHAR(4000) NOT NULL
,[alias_symbol]                                NVARCHAR(4000) NOT NULL
,[alias_name]                                  NVARCHAR(4000) NOT NULL
,[prev_symbol]                                 NVARCHAR(4000) NOT NULL
,[prev_name]                                   NVARCHAR(4000) NOT NULL
,[gene_family]                                 NVARCHAR(4000) NOT NULL
,[gene_family_id]                              NVARCHAR(4000) NOT NULL
,[date_approved_reserved]                      NVARCHAR(4000) NOT NULL
,[date_symbol_changed]                         NVARCHAR(4000) NOT NULL
,[date_name_changed]                           NVARCHAR(4000) NOT NULL
,[date_modified]                               NVARCHAR(4000) NOT NULL
,[entrez_id]                                   NVARCHAR(4000) NOT NULL
,[ensembl_gene_id]                             NVARCHAR(4000) NOT NULL
,[vega_id]                                     NVARCHAR(4000) NOT NULL
,[ucsc_id]                                     NVARCHAR(4000) NOT NULL
,[ena]                                         NVARCHAR(4000) NOT NULL
,[refseq_accession]                            NVARCHAR(4000) NOT NULL
,[ccds_id]                                     NVARCHAR(4000) NOT NULL
,[uniprot_ids]                                 NVARCHAR(4000) NOT NULL
,[pubmed_id]                                   NVARCHAR(4000) NOT NULL
,[mgd_id]                                      NVARCHAR(4000) NOT NULL
,[rgd_id]                                      NVARCHAR(4000) NOT NULL
,[lsdb]                                        NVARCHAR(4000) NOT NULL
,[cosmic]                                      NVARCHAR(4000) NOT NULL
,[omim_id]                                     NVARCHAR(4000) NOT NULL
,[mirbase]                                     NVARCHAR(4000) NOT NULL
,[homeodb]                                     NVARCHAR(4000) NOT NULL
,[snornabase]                                  NVARCHAR(4000) NOT NULL
,[bioparadigms_slc]                            NVARCHAR(4000) NOT NULL
,[orphanet]                                    NVARCHAR(4000) NOT NULL
,[pseudogene.org]                              NVARCHAR(4000) NOT NULL
,[horde_id]                                    NVARCHAR(4000) NOT NULL
,[merops]                                      NVARCHAR(4000) NOT NULL
,[imgt]                                        NVARCHAR(4000) NOT NULL
,[iuphar]                                      NVARCHAR(4000) NOT NULL
,[kznf_gene_catalog]                           NVARCHAR(4000) NOT NULL
,[mamit-trnadb]                                NVARCHAR(4000) NOT NULL
,[cd]                                          NVARCHAR(4000) NOT NULL
,[lncrnadb]                                    NVARCHAR(4000) NOT NULL
,[enzyme_id]                                   NVARCHAR(4000) NOT NULL
,[intermediate_filament_db]                    NVARCHAR(4000) NOT NULL
,[rna_central_ids]                             NVARCHAR(4000) NOT NULL
)

DECLARE @bulk_cmd NVARCHAR(4000);
SET @bulk_cmd = 'BULK INSERT [hgnc_complete_set_raw]
FROM ''/src/hgnc_complete_set.txt''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ''\t'',
    ROWTERMINATOR = '''+CHAR(10)+'''
)'
EXEC (@bulk_cmd)

INSERT INTO [dbo].[hgnc_complete_set] WITH (TABLOCK)
SELECT DISTINCT [symbol]
               ,[entrez_id]
  FROM [hgnc_complete_set_raw]
 WHERE [entrez_id]<>''

CREATE NONCLUSTERED INDEX [idx_hgnc_complete_set_symbol_entrez_id]
    ON [dbo].[hgnc_complete_set]([symbol] ASC, [entrez_id] ASC)

DROP TABLE [hgnc_complete_set_raw]
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"hgnc successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].[gene_info] (
    [GeneID]      INT             NOT NULL,
    [description] NVARCHAR (4000) NULL
)

CREATE TABLE [dbo].[gene_info_raw] (
 [tax_id]                                NVARCHAR(4000) NOT NULL
,[GeneID]                                NVARCHAR(4000) NOT NULL
,[Symbol]                                NVARCHAR(4000) NOT NULL
,[LocusTag]                              NVARCHAR(4000) NOT NULL
,[Synonyms]                              NVARCHAR(4000) NOT NULL
,[dbXrefs]                               NVARCHAR(4000) NOT NULL
,[chromosome]                            NVARCHAR(4000) NOT NULL
,[map_location]                          NVARCHAR(4000) NOT NULL
,[description]                           NVARCHAR(4000) NOT NULL
,[type_of_gene]                          NVARCHAR(4000) NOT NULL
,[Symbol_from_nomenclature_authority]    NVARCHAR(4000) NOT NULL
,[Full_name_from_nomenclature_authority] NVARCHAR(4000) NOT NULL
,[Nomenclature_status]                   NVARCHAR(4000) NOT NULL
,[Other_designations]                    NVARCHAR(MAX) NOT NULL
,[Modification_date]                     NVARCHAR(4000) NOT NULL
,[Feature_type]                          NVARCHAR(4000) NOT NULL
)

DECLARE @bulk_cmd NVARCHAR(4000);
SET @bulk_cmd = 'BULK INSERT [gene_info_raw]
FROM ''/src/gene_info''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ''\t'',
    ROWTERMINATOR = '''+CHAR(10)+'''
)'
EXEC (@bulk_cmd)

INSERT INTO [dbo].[gene_info] WITH (TABLOCK)
SELECT DISTINCT [GeneID]
               ,[description]
  FROM [dbo].[gene_info_raw]

CREATE NONCLUSTERED INDEX [idx_gene_info_GeneID]
    ON [dbo].[gene_info]([GeneID] ASC)
    INCLUDE([description])

DROP TABLE [gene_info_raw]
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"gene_info successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].[gene2pubmed] (
    [GeneID]    INT NOT NULL,
    [PubMed_ID] INT NOT NULL
)

CREATE TABLE [dbo].[gene2pubmed_raw] (
 [tax_id]    NVARCHAR(4000) NOT NULL
,[GeneID]    NVARCHAR(4000) NOT NULL
,[PubMed_ID] NVARCHAR(4000) NOT NULL
)

DECLARE @bulk_cmd NVARCHAR(4000);
SET @bulk_cmd = 'BULK INSERT [gene2pubmed_raw]
FROM ''/src/gene2pubmed''
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ''\t'',
ROWTERMINATOR = '''+CHAR(10)+'''
)'
EXEC (@bulk_cmd)

INSERT INTO [dbo].[gene2pubmed] WITH (TABLOCK)
SELECT DISTINCT [GeneID]
               ,[PubMed_ID]
  FROM [dbo].[gene2pubmed_raw]

CREATE NONCLUSTERED INDEX [IX_gene2pubmed_PubMed_ID_GeneID]
    ON [dbo].[gene2pubmed]([PubMed_ID] ASC, [GeneID] ASC)

CREATE NONCLUSTERED INDEX [IX_gene2pubmed_GeneID_PubMed_ID]
    ON [dbo].[gene2pubmed]([GeneID] ASC, [PubMed_ID] ASC)

DROP TABLE [gene2pubmed_raw]
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"gene2pubmed successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].[gene2pubtator3] (
    [PMID]      BIGINT NOT NULL,
    [NCBI_Gene] BIGINT NOT NULL
)

CREATE TABLE [dbo].[gene2pubtator3_raw] (
 [PMID]       NVARCHAR(4000) NOT NULL
,[Type]       NVARCHAR(4000) NOT NULL
,[Concept ID] NVARCHAR(4000) NOT NULL
,[Mentions]   NVARCHAR(4000) NOT NULL
,[Resource]   NVARCHAR(4000) NOT NULL
)

DECLARE @bulk_cmd NVARCHAR(4000);
SET @bulk_cmd = 'BULK INSERT [gene2pubtator3_raw]
FROM ''/src/gene2pubtator3''
WITH (
FIRSTROW = 1,
FIELDTERMINATOR = ''\t'',
ROWTERMINATOR = '''+CHAR(10)+'''
)'
EXEC (@bulk_cmd)

INSERT INTO [dbo].[gene2pubtator3] WITH (TABLOCK)
SELECT DISTINCT [PMID]
              , LTRIM(RTRIM(VALUE))
  FROM [dbo].[gene2pubtator3_raw]
 CROSS APPLY STRING_SPLIT([Concept ID], ';')

CREATE NONCLUSTERED INDEX [IX_gene2pubtator3_PMID_NCBI_Gene]
    ON [dbo].[gene2pubtator3]([PMID] ASC, [NCBI_Gene] ASC)

CREATE NONCLUSTERED INDEX [IX_gene2pubtator3_NCBI_Gene_PMID]
    ON [dbo].[gene2pubtator3]([NCBI_Gene] ASC, [PMID] ASC)

DROP TABLE [gene2pubtator3_raw]
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"gene2pubtator3 successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].[pubmed] (
    [id]           UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
    [PMID]         INT              NOT NULL,
    [Version]      INT              NOT NULL,
    [ArticleTitle] NVARCHAR (MAX)   NULL,
    [AbstractText] XML              NULL,
    CONSTRAINT [PK_pubmed] PRIMARY KEY NONCLUSTERED ([id] ASC)
)
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"pubmed successfully.");
        }

        string folderPath = @"/src";
        var files = Directory.EnumerateFiles(folderPath, "*.xml");

        Parallel.ForEach(files, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount }, filepath =>
        {
            string i = Path.GetFileNameWithoutExtension(filepath);
            var bulk_cmd = $@"
DROP TABLE IF EXISTS [dbo].pubmed_raw{i}

CREATE TABLE pubmed_raw{i}(XmlCol xml)

INSERT pubmed_raw{i} WITH (TABLOCK)
SELECT CONVERT(xml, BulkColumn, 2) FROM
OPENROWSET(Bulk '{filepath}', SINGLE_BLOB) [rowsetresults]

DECLARE @XML AS XML, @hDoc AS INT, @SQL NVARCHAR (MAX)
SELECT @XML = XmlCol FROM pubmed_raw{i}

EXEC sp_xml_preparedocument @hDoc OUTPUT, @XML

INSERT INTO [dbo].pubmed (PMID, Version, ArticleTitle, AbstractText)
SELECT PMID, Version, ArticleTitle, AbstractText
FROM OPENXML(@hDoc, 'PubmedArticleSet/PubmedArticle/MedlineCitation')
WITH (
    PMID int 'PMID[1]/text()',
    [version] int 'PMID[1]/@Version/text()',
    ArticleTitle [nvarchar](max) 'Article/ArticleTitle[1]/text()',
    AbstractText XML 'Article/Abstract'
)

EXEC sp_xml_removedocument @hDoc

DROP TABLE pubmed_raw{i}
";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(bulk_cmd, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                        Console.WriteLine($"{i} successfully.");
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("An error occurred: " + ex.Message);
                    throw;
                }
            }
        });

        sqlQuery = @"
CREATE NONCLUSTERED INDEX [idx_pubmed_PMID_Version]
    ON [dbo].[pubmed]([PMID] ASC, [Version] DESC)
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"pubmed index successfully.");
        }

        sqlQuery = @"
DELETE FROM [pubmed]
 WHERE [pubmed].[version] <>
  (SELECT MAX([version])
     FROM [pubmed] i
    WHERE i.pmid = [pubmed].pmid GROUP BY pmid)
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"delete old pubmed successfully.");
        }

        sqlQuery = @"
CREATE FULLTEXT CATALOG [FullTextCatalog]
    WITH ACCENT_SENSITIVITY = ON
    AUTHORIZATION [dbo]
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"FullTextCatalog successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].gene2pubmed_combine (
    [path_locator] NVARCHAR (255) NOT NULL,
    [file_stream]  NVARCHAR (MAX) NULL
)

CREATE UNIQUE INDEX [idx_gene2pubmed_combine]
    ON [dbo].[gene2pubmed_combine]([path_locator] ASC)

CREATE FULLTEXT INDEX ON [dbo].gene2pubmed_combine(file_stream)
    KEY INDEX idx_gene2pubmed_combine
    on FullTextCatalog
    WITH CHANGE_TRACKING AUTO
";

        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"CREATE gene2pubmed_combine successfully.");
        }

        sqlQuery = @"
CREATE TABLE [dbo].gene2pubtator_combine (
    [path_locator] NVARCHAR (255) NOT NULL,
    [file_stream]  NVARCHAR (MAX) NULL
)

CREATE UNIQUE INDEX [idx_gene2pubtator_combine]
    ON [dbo].[gene2pubtator_combine]([path_locator] ASC);

CREATE FULLTEXT INDEX ON [dbo].gene2pubtator_combine(file_stream)
    KEY INDEX idx_gene2pubtator_combine
    on FullTextCatalog
    WITH CHANGE_TRACKING AUTO
";
        using (SqlCommand command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            Console.WriteLine($"CREATE gene2pubtator_combine successfully.");
        }

        var symbols = new List<string>();
        string query = "SELECT [symbol], [entrez_id] FROM [dbo].[hgnc_complete_set]";
        using (var command = new SqlCommand(query, connection))
        using (var dataReader = command.ExecuteReader())
        {
            
            while (dataReader.Read())
            {
                string symbol = dataReader.GetString(0);
                symbols.Add(symbol);
            }
        }

        Directory.CreateDirectory("gene2pubmed_combine");
        Directory.CreateDirectory("gene2pubtator_combine");
        Parallel.ForEach(symbols, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount }, symbol =>
        {
            ProcessData(symbol, "gene2pubmed_combine", connectionString);
            ProcessData(symbol, "gene2pubtator_combine", connectionString, isGene2pubtator3: true);

            InsertData(symbol, "gene2pubmed_combine", connectionString);
            InsertData(symbol, "gene2pubtator_combine", connectionString);
        });
        Directory.Delete("gene2pubmed_combine", true);
        Directory.Delete("gene2pubtator_combine", true);

        Console.WriteLine($"combine successfully.");
    }
    catch (SqlException ex)
    {
        Console.WriteLine("An error occurred: " + ex.Message);
        return 1;
    }
}

return 0;

void InsertData(string symbol, string folderName, string connectionString)
{
    var filePath = Path.Combine(folderName, $"{symbol}.txt");
    string fileContent = File.ReadAllText(filePath);
    string insertQuery = $"INSERT INTO {folderName}(path_locator, file_stream) VALUES (@symbol, @content)";

    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        connection.Open();
        using (SqlCommand command = new SqlCommand(insertQuery, connection))
        {
            command.CommandTimeout = 0;

            command.Parameters.AddWithValue("@symbol", symbol);
            command.Parameters.AddWithValue("@content", fileContent);

            command.ExecuteNonQuery();
        }
    }

    File.Delete(filePath);
}

void ProcessData(string symbol, string folderName, string connectionString, bool isGene2pubtator3 = false)
{
    string sqlQuery = isGene2pubtator3
        ? "SELECT [ArticleTitle], [AbstractText] FROM [dbo].[hgnc_complete_set] " +
          "JOIN [dbo].[gene2pubtator3] ON [dbo].[gene2pubtator3].[NCBI_Gene] = [dbo].[hgnc_complete_set].[entrez_id] " +
          "JOIN [dbo].[pubmed] ON [dbo].[pubmed].[PMID] = [dbo].[gene2pubtator3].[PMID] " +
          "WHERE [symbol] = @symbol;"
        : "SELECT [ArticleTitle], [AbstractText] FROM [dbo].[hgnc_complete_set] " +
          "JOIN [dbo].[gene2pubmed] ON [dbo].[gene2pubmed].[GeneID] = [dbo].[hgnc_complete_set].[entrez_id] " +
          "JOIN [dbo].[pubmed] ON [dbo].[pubmed].[PMID] = [dbo].[gene2pubmed].[PubMed_ID] " +
          "WHERE [symbol] = @symbol;";

    using (var connection = new SqlConnection(connectionString))
    {
        connection.Open();
        using (var command = new SqlCommand(sqlQuery, connection))
        {
            command.CommandTimeout = 0;
            command.Parameters.Add(new SqlParameter("symbol", symbol));
            using (var dataReader = command.ExecuteReader())
            {
                var filePath = Path.Combine(folderName, $"{symbol}.txt");
                using (var fileStream = new FileStream(filePath, FileMode.Create))
                using (var streamWriter = new StreamWriter(fileStream))
                {
                    while (dataReader.Read())
                    {
                        if (!dataReader.IsDBNull(0))
                        {
                            streamWriter.Write(dataReader.GetString(0));
                            streamWriter.Write(" ");
                        }

                        if (!dataReader.IsDBNull(1))
                        {
                            using (var xmlReader = dataReader.GetXmlReader(1))
                            {
                                while (xmlReader.NodeType != XmlNodeType.Element)
                                {
                                    xmlReader.Read();
                                }

                                var text = XElement.Load(xmlReader)
                                    .Elements("AbstractText")
                                    .Select(x => x.Value)
                                    .Aggregate((a, b) => a + " " + b)
                                    .Trim();
                                streamWriter.Write(text);
                                streamWriter.Write(" ");
                            }
                        }
                    }
                }
            }
        }
    }
}