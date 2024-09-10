using Microsoft.Data.SqlClient;
using System.Text;
using System.Text.Json.Serialization;

namespace Webapi
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateSlimBuilder(args);

            builder.Services.ConfigureHttpJsonOptions(options =>
            {
                options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
            });

            var app = builder.Build();

            var api = app.MapGroup("/");
            api.MapPost("/VPHandler.ashx", (HttpContext context) =>
            {
                string? genes = context.Request.Form["genes"];
                string? keywords = context.Request.Form["keywords"];
                string? type = context.Request.Form["type"];

                var sqlEntrez = @"
    SELECT [symbol] AS [genesymbol]
         , 'EntrezGene' AS [sourcetype]
         , CAST([hgnc_complete_set].[entrez_id] AS NVARCHAR) AS [NBK_id]
         , [gene_info].[description] AS [GR_Title]
         , [KEY_TBL].RANK
      FROM [dbo].[gene2pubmed_combine] AS [FT_TBL]
           INNER JOIN FREETEXTTABLE([dbo].[gene2pubmed_combine], [file_stream], @keywords) AS [KEY_TBL]
           ON [FT_TBL].[path_locator] = [KEY_TBL].[KEY]
           INNER JOIN [dbo].[hgnc_complete_set]
           ON [hgnc_complete_set].[symbol] = [FT_TBL].[path_locator]
           INNER JOIN [dbo].[gene_info]
           ON [dbo].[gene_info].[GeneID] = [hgnc_complete_set].[entrez_id]
";
                var sqlPubtator = @"
    SELECT [symbol] AS [genesymbol]
         , 'PubTator' AS [sourcetype]
         , CAST([hgnc_complete_set].[entrez_id] AS NVARCHAR) AS [NBK_id]
         , [gene_info].[description] AS [GR_Title]
         , [KEY_TBL].RANK
      FROM [dbo].[gene2pubtator_combine] AS [FT_TBL]
           INNER JOIN FREETEXTTABLE([dbo].[gene2pubtator_combine], [file_stream], @keywords) AS [KEY_TBL]
           ON [FT_TBL].[path_locator] = [KEY_TBL].[KEY]
           INNER JOIN [dbo].[hgnc_complete_set]
           ON [hgnc_complete_set].[symbol] = [FT_TBL].[path_locator]
           INNER JOIN [dbo].[gene_info]
           ON [dbo].[gene_info].[GeneID] = [hgnc_complete_set].[entrez_id]
";
                var sql_db = string.Empty;

                switch (type)
                {
                    case "EntrezGene":
                        sql_db = sqlEntrez;
                        break;
                    case "PubTator":
                        sql_db = sqlPubtator;
                        break;
                    default:
                        sql_db = $@"
{sqlEntrez}
    UNION
{sqlPubtator}
";
                        break;
                }

                var sql = $@"
WITH tempTT([genesymbol]
          , [sourcetype]
          , [NBK_id]
          , [GR_Title]
          , RANK
          , [groupOrder])
AS
(
SELECT [rty].[genesymbol]
     , [rty].[sourcetype]
     , [rty].[NBK_id]
     , [rty].[GR_Title]
     , [rty].RANK
     , RANK() OVER(PARTITION BY [rty].[genesymbol]
                       ORDER BY [rty].RANK DESC) AS [groupOrder]
FROM
(
{sql_db}
) AS [rty]
";

                var geneList = genes.Split('\n').Select(x => x.Trim()).Distinct().Where(x => !string.Empty.Equals(x)).ToList();
                if (geneList.Any())
                {
                    var sb = new StringBuilder();
                    sb.Append(@"WHERE [rty].[genesymbol] IN(");

                    for (int i = 0; i < geneList.Count; i++)
                    {
                        sb.Append($"@p{i},");
                    }

                    sb.Remove(sb.Length - 1, 1);
                    sb.Append("))");

                    sql += sb.ToString();
                }
                else
                {
                    sql += ")";
                }

                sql += @"
SELECT [gg].[groupno]
     , [tempTT].[genesymbol]
     , [tempTT].[sourcetype]
     , [tempTT].[NBK_id]
     , [tempTT].[GR_Title]
     , [tempTT].RANK
FROM
(
SELECT ROW_NUMBER() OVER(ORDER BY MAX([a].rank) DESC
                                , AVG([a].rank) DESC) AS [groupno]
     , [a].[genesymbol]
  FROM [tempTT] AS [a]
 GROUP BY [a].[genesymbol]
) AS [gg]
INNER JOIN [tempTT]
ON [tempTT].[genesymbol] = [gg].[genesymbol]
INNER JOIN (
    SELECT [genesymbol], MAX(RANK) AS [maxrank]
    FROM [tempTT]
    GROUP BY [genesymbol]
) AS [b] ON [tempTT].[genesymbol] = [b].[genesymbol] 
AND [tempTT].[RANK] = [b].[maxrank]
ORDER BY [gg].[groupno]
    , [tempTT].[groupOrder];
";

                var configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                var connectionString = configuration.GetConnectionString("DefaultConnection");
                using (var conn = new SqlConnection(connectionString))
                {
                    using (var cmd = new SqlCommand(sql, conn))
                    {
                        cmd.CommandTimeout = 0;
                        cmd.Parameters.AddWithValue("@keywords", keywords);

                        if (geneList.Any())
                        {
                            for (int i = 0; i < geneList.Count; i++)
                            {
                                cmd.Parameters.AddWithValue($"@p{i}", geneList[i]);
                            }
                        }

                        conn.Open();
                        using (var dr = cmd.ExecuteReader())
                        {
                            if (dr.HasRows)
                            {
                                var gnoOrdinal = dr.GetOrdinal("groupno");
                                var nameOrdinal = dr.GetOrdinal("genesymbol");
                                var sourcetypeOrdinal = dr.GetOrdinal("sourcetype");
                                var idOrdinal = dr.GetOrdinal("NBK_id");
                                var titleOrdinal = dr.GetOrdinal("GR_Title");
                                var rankOrdinal = dr.GetOrdinal("RANK");

                                var rtnList = new List<Result>();

                                while (dr.Read())
                                {
                                    rtnList.Add(new Result(
                                        gn: dr.GetInt64(gnoOrdinal),
                                        geneName: dr.GetString(nameOrdinal),
                                        sourcetype: dr.GetString(sourcetypeOrdinal),
                                        nbkid: dr.GetString(idOrdinal),
                                        title: dr.GetString(titleOrdinal),
                                        rank: dr.GetInt32(rankOrdinal)
                                    ));
                                }

                                return Results.Ok(rtnList.ToArray());
                            }
                        }
                    }
                }

                return Results.Ok(new Result[] { });
            });

            app.Run();
        }
    }

    public record Result
    (
        long gn,
        string geneName,
        string sourcetype,
        string nbkid,
        string title,
        int rank
    );

    [JsonSerializable(typeof(Result[]))]
    internal partial class AppJsonSerializerContext : JsonSerializerContext
    {

    }
}
